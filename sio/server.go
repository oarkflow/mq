package sio

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const ( //                        ASCII chars
	startOfHeaderByte uint8 = 1 // SOH
	startOfDataByte         = 2 // STX

	// SubProtocol is the official sacrificial-socket sub protocol
	SubProtocol string = "sac-sock"
)

type event struct {
	eventHandler func(*Socket, []byte)
	eventName    string
}

// Config specifies parameters for upgrading an HTTP connection to a
// WebSocket connection.
//
// It is safe to call Config's methods concurrently.
type Config struct {
	WriteBufferPool   websocket.BufferPool
	Error             func(w http.ResponseWriter, r *http.Request, status int, reason error)
	CheckOrigin       func(r *http.Request) bool
	Subprotocols      []string
	HandshakeTimeout  time.Duration
	ReadBufferSize    int
	WriteBufferSize   int
	EnableCompression bool
}

// Server manages the coordination between
// sockets, rooms, events and the socket hub
// add my own custom field
type Server struct {
	hub              *hub
	events           map[string]*event
	onConnectFunc    func(*Socket) error
	onDisconnectFunc func(*Socket) error
	onError          func(*Socket, error)
	l                *sync.RWMutex
	upgrader         *websocket.Upgrader
}

// New creates a new instance of Server
func New(cfg ...Config) *Server {
	var config Config
	upgrader := DefaultUpgrader()
	if len(cfg) > 0 {
		config = cfg[0]
	}
	if config.CheckOrigin != nil {
		upgrader.CheckOrigin = config.CheckOrigin
	}
	if config.HandshakeTimeout != 0 {
		upgrader.HandshakeTimeout = config.HandshakeTimeout
	}
	if config.ReadBufferSize != 0 {
		upgrader.ReadBufferSize = config.ReadBufferSize
	}
	if config.WriteBufferSize != 0 {
		upgrader.WriteBufferSize = config.WriteBufferSize
	}
	if len(config.Subprotocols) > 0 {
		upgrader.Subprotocols = config.Subprotocols
	}
	if config.Error != nil {
		upgrader.Error = config.Error
	}
	upgrader.EnableCompression = config.EnableCompression
	s := &Server{
		hub:      newHub(),
		events:   make(map[string]*event),
		l:        &sync.RWMutex{},
		upgrader: upgrader,
	}

	return s
}

func (serv *Server) ShutdownWithSignal() {
	c := make(chan bool)
	serv.EnableSignalShutdown(c)
	go func() {
		<-c
		os.Exit(0)
	}()
}

// EnableSignalShutdown listens for linux syscalls SIGHUP, SIGINT, SIGTERM, SIGQUIT, SIGKILL and
// calls the Server.Shutdown() to perform a clean shutdown. true will be passed into complete
// after the Shutdown proccess is finished
func (serv *Server) EnableSignalShutdown(complete chan<- bool) {
	c := make(chan os.Signal, 1)
	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGKILL)

	go func() {
		<-c
		complete <- serv.Shutdown()
	}()
}

func (serv *Server) Lock() {
	serv.l.Lock()
}

func (serv *Server) Unlock() {
	serv.l.Unlock()
}

func (serv *Server) RoomSocketList(id string) map[string]*Socket {
	sockets := make(map[string]*Socket)
	if room, exists := serv.hub.rooms[id]; exists {
		room.l.Lock()
		for id, socket := range room.sockets {
			sockets[id] = socket
		}
		room.l.Unlock()
	}
	return sockets
}

func (serv *Server) SocketList() map[string]*Socket {
	sockets := make(map[string]*Socket)
	serv.l.Lock()
	for id, socket := range serv.hub.sockets {
		sockets[id] = socket
	}
	serv.l.Unlock()
	return sockets
}

// Shutdown closes all active sockets and triggers the Shutdown()
// method on any Adapter that is currently set.
func (serv *Server) Shutdown() bool {
	slog.Info("shutting down")
	// complete := serv.hub.shutdown()

	serv.hub.shutdownCh <- true
	socketList := <-serv.hub.socketList

	for _, s := range socketList {
		s.Close()
	}

	if serv.hub.multihomeEnabled {
		slog.Info("shutting down multihome backend")
		serv.hub.multihomeBackend.Shutdown()
		slog.Info("backend shutdown")
	}

	slog.Info("shutdown")
	return true
}

// EventHandler is an interface for registering events using SockerServer.OnEvent
type EventHandler interface {
	HandleEvent(*Socket, []byte)
	EventName() string
}

// On registers event functions to be called on individual Socket connections
// when the server's socket receives an Emit from the client's socket.
//
// Any event functions registered with On, must be safe for concurrent use by multiple
// go routines
func (serv *Server) On(eventName string, handleFunc func(*Socket, []byte)) {
	serv.events[eventName] = &event{eventName: eventName, eventHandler: handleFunc} // you think you can handle the func?
}

// OnEvent has the same functionality as On, but accepts
// an EventHandler interface instead of a handler function.
func (serv *Server) OnEvent(h EventHandler) {
	serv.On(h.EventName(), h.HandleEvent)
}

// OnConnect registers an event function to be called whenever a new Socket connection
// is created
func (serv *Server) OnConnect(handleFunc func(*Socket) error) {
	serv.onConnectFunc = handleFunc
}

// OnError registers an event function to be called whenever a new Socket connection
// is created
func (serv *Server) OnError(handleFunc func(*Socket, error)) {
	serv.onError = handleFunc
}

// OnDisconnect registers an event function to be called as soon as a Socket connection
// is closed
func (serv *Server) OnDisconnect(handleFunc func(*Socket) error) {
	serv.onDisconnectFunc = handleFunc
}

// WebHandler returns a http.Handler to be passed into http.Handle
//
// Depricated: The Server struct now satisfies the http.Handler interface, use that instead
func (serv *Server) WebHandler() http.Handler {
	return serv
}

// ServeHTTP will upgrade a http request to a websocket using the sac-sock subprotocol
func (serv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := serv.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error(err.Error())
		return
	}
	request := r.Clone(context.Background())
	serv.loop(ws, request)
}

// DefaultUpgrader returns a websocket upgrader suitable for creating sacrificial-socket websockets.
func DefaultUpgrader() *websocket.Upgrader {
	return &websocket.Upgrader{
		Subprotocols: []string{SubProtocol},
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
}

// SetUpgrader sets the websocket.Upgrader used by the Server.
func (serv *Server) SetUpgrader(u *websocket.Upgrader) {
	serv.upgrader = u
}

// SetMultihomeBackend registers an Adapter interface and calls its Init() method
func (serv *Server) SetMultihomeBackend(b Adapter) {
	serv.hub.setMultihomeBackend(b)
}

// ToRoom dispatches an event to all Sockets in the specified room.
func (serv *Server) ToRoom(roomName, eventName string, data any) {
	serv.hub.toRoom(&RoomMsg{RoomName: roomName, EventName: eventName, Data: data})
}

// ToRoomExcept dispatches an event to all Sockets in the specified room.
func (serv *Server) ToRoomExcept(roomName string, except []string, eventName string, data any) {
	serv.hub.toRoom(&RoomMsg{RoomName: roomName, EventName: eventName, Data: data, Except: except})
}

// Broadcast dispatches an event to all Sockets on the Server.
func (serv *Server) Broadcast(eventName string, data any) {
	serv.hub.broadcast(&BroadcastMsg{EventName: eventName, Data: data})
}

// BroadcastExcept dispatches an event to all Sockets on the Server.
func (serv *Server) BroadcastExcept(except []string, eventName string, data any) {
	serv.hub.broadcast(&BroadcastMsg{EventName: eventName, Except: except, Data: data})
}

// ToSocket dispatches an event to the specified socket ID.
func (serv *Server) ToSocket(socketID, eventName string, data any) {
	serv.ToRoom("__socket_id:"+socketID, eventName, data)
}

// loop handles all the coordination between new sockets
// reading frames and dispatching events
func (serv *Server) loop(ws *websocket.Conn, r *http.Request) {
	s := newSocket(serv, ws, r)
	slog.Info("connected", "id", s.ID())

	defer s.Close()

	s.Join("__socket_id:" + s.ID())

	serv.l.RLock()
	e := serv.onConnectFunc
	serv.l.RUnlock()

	if e != nil {
		err := e(s)
		if err != nil && serv.onError != nil {
			serv.onError(s, err)
		}
	}

	for {
		msg, err := s.receive()
		if ignorableError(err) {
			return
		}
		if err != nil {
			slog.Error(err.Error())
			return
		}

		eventName := ""
		contentIdx := 0

		for idx, chr := range msg {
			if chr == startOfDataByte {
				eventName = string(msg[:idx])
				contentIdx = idx + 1
				break
			}
		}
		if eventName == "" {
			slog.Warn("no event to dispatch")
			continue
		}

		serv.l.RLock()
		e, exists := serv.events[eventName]
		serv.l.RUnlock()

		if exists {
			go e.eventHandler(s, msg[contentIdx:])
		}
	}
}

func ignorableError(err error) bool {
	// not an error
	if err == nil {
		return false
	}

	return err == io.EOF ||
		websocket.IsCloseError(err, 1000) ||
		websocket.IsCloseError(err, 1001) ||
		strings.HasSuffix(err.Error(), "use of closed network connection")
}
