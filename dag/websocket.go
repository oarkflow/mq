package dag

import (
	"github.com/oarkflow/json"

	"github.com/oarkflow/mq/sio"
)

func WsEvents(s *sio.Server) {
	s.On("join", join)
	s.On("message", message)
}

func join(s *sio.Socket, data []byte) {
	currentRooms := s.GetRooms()
	for _, room := range currentRooms {
		s.Leave(room)
	}
	s.Join(string(data))
	s.Emit("joinedRoom", string(data))
}

type msg struct {
	Room    string
	Message string
}

func message(s *sio.Socket, data []byte) {
	var m msg
	json.Unmarshal(data, &m)
	s.ToRoom(m.Room, "message", m.Message)
}
