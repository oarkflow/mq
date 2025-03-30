package mq

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/jsonparser"
	"github.com/oarkflow/mq/utils"
)

type Processor interface {
	ProcessTask(ctx context.Context, msg *Task) Result
	Consume(ctx context.Context) error
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	Stop(ctx context.Context) error
	Close() error
	GetKey() string
	SetKey(key string)
	GetType() string
}

type Consumer struct {
	conn    net.Conn
	handler Handler
	pool    *Pool
	opts    *Options
	id      string
	queue   string
}

func NewConsumer(id string, queue string, handler Handler, opts ...Option) *Consumer {
	options := SetupOptions(opts...)
	return &Consumer{
		id:      id,
		opts:    options,
		queue:   queue,
		handler: handler,
	}
}

func (c *Consumer) send(ctx context.Context, conn net.Conn, msg *codec.Message) error {
	return codec.SendMessage(ctx, conn, msg)
}

func (c *Consumer) receive(ctx context.Context, conn net.Conn) (*codec.Message, error) {
	return codec.ReadMessage(ctx, conn)
}

func (c *Consumer) Close() error {
	c.pool.Stop()
	err := c.conn.Close()
	log.Printf("CONSUMER - Connection closed for consumer: %s", c.id)
	return err
}

func (c *Consumer) GetKey() string {
	return c.id
}

func (c *Consumer) GetType() string {
	return "consumer"
}

func (c *Consumer) SetKey(key string) {
	c.id = key
}

func (c *Consumer) Metrics() Metrics {
	return c.pool.Metrics()
}

func (c *Consumer) subscribe(ctx context.Context, queue string) error {
	headers := HeadersWithConsumerID(ctx, c.id)
	msg := codec.NewMessage(consts.SUBSCRIBE, utils.ToByte("{}"), queue, headers)
	if err := c.send(ctx, c.conn, msg); err != nil {
		return fmt.Errorf("error while trying to subscribe: %v", err)
	}
	return c.waitForAck(ctx, c.conn)
}

func (c *Consumer) OnClose(_ context.Context, _ net.Conn) error {
	fmt.Println("Consumer closed")
	return nil
}

func (c *Consumer) OnError(_ context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from connection:", err, conn.RemoteAddr())
}

func (c *Consumer) OnMessage(ctx context.Context, msg *codec.Message, conn net.Conn) error {
	switch msg.Command {
	case consts.PUBLISH:
		c.ConsumeMessage(ctx, msg, conn)
	case consts.CONSUMER_PAUSE:
		err := c.Pause(ctx)
		if err != nil {
			log.Printf("Unable to pause consumer: %v", err)
		}
		return err
	case consts.CONSUMER_RESUME:
		err := c.Resume(ctx)
		if err != nil {
			log.Printf("Unable to resume consumer: %v", err)
		}
		return err
	case consts.CONSUMER_STOP:
		err := c.Stop(ctx)
		if err != nil {
			log.Printf("Unable to stop consumer: %v", err)
		}
		return err
	case consts.CONSUMER_UPDATE:
		err := c.Update(ctx, msg.Payload)
		if err != nil {
			log.Printf("Unable to update consumer: %v", err)
		}
		return err
	default:
		log.Printf("CONSUMER - UNKNOWN_COMMAND ~> %s on %s", msg.Command, msg.Queue)
	}
	return nil
}

func (c *Consumer) sendMessageAck(ctx context.Context, msg *codec.Message, conn net.Conn) {
	headers := HeadersWithConsumerIDAndQueue(ctx, c.id, msg.Queue)
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	reply := codec.NewMessage(consts.MESSAGE_ACK, utils.ToByte(fmt.Sprintf(`{"id":"%s"}`, taskID)), msg.Queue, headers)
	if err := c.send(ctx, conn, reply); err != nil {
		fmt.Printf("failed to send MESSAGE_ACK for queue %s: %v", msg.Queue, err)
	}
}

func (c *Consumer) ConsumeMessage(ctx context.Context, msg *codec.Message, conn net.Conn) {
	c.sendMessageAck(ctx, msg, conn)
	if msg.Payload == nil {
		log.Printf("Received empty message payload")
		return
	}
	var task Task
	err := json.Unmarshal(msg.Payload, &task)
	if err != nil {
		log.Printf("Error unmarshalling message: %v", err)
		return
	}
	ctx = SetHeaders(ctx, map[string]string{consts.QueueKey: msg.Queue})
	if err := c.pool.EnqueueTask(ctx, &task, 1); err != nil {
		c.sendDenyMessage(ctx, task.ID, msg.Queue, err)
		return
	}
}

func (c *Consumer) ProcessTask(ctx context.Context, msg *Task) Result {
	defer RecoverPanic(RecoverTitle)
	queue, _ := GetQueue(ctx)
	if msg.Topic == "" && queue != "" {
		msg.Topic = queue
	}
	result := c.handler(ctx, msg)
	result.Topic = msg.Topic
	result.TaskID = msg.ID
	return result
}

func (c *Consumer) OnResponse(ctx context.Context, result Result) error {
	if result.Status == "PENDING" && c.opts.respondPendingResult {
		return nil
	}
	headers := HeadersWithConsumerIDAndQueue(ctx, c.id, result.Topic)
	if result.Status == "" {
		if result.Error != nil {
			result.Status = "FAILED"
		} else {
			result.Status = "SUCCESS"
		}
	}
	bt, _ := json.Marshal(result)
	reply := codec.NewMessage(consts.MESSAGE_RESPONSE, bt, result.Topic, headers)
	if err := c.send(ctx, c.conn, reply); err != nil {
		return fmt.Errorf("failed to send MESSAGE_RESPONSE: %v", err)
	}
	return nil
}

func (c *Consumer) sendDenyMessage(ctx context.Context, taskID, queue string, err error) {
	headers := HeadersWithConsumerID(ctx, c.id)
	reply := codec.NewMessage(consts.MESSAGE_DENY, utils.ToByte(fmt.Sprintf(`{"id":"%s", "error":"%s"}`, taskID, err.Error())), queue, headers)
	if sendErr := c.send(ctx, c.conn, reply); sendErr != nil {
		log.Printf("failed to send MESSAGE_DENY for task %s: %v", taskID, sendErr)
	}
}

func (c *Consumer) attemptConnect() error {
	var err error
	delay := c.opts.initialDelay
	for i := 0; i < c.opts.maxRetries; i++ {
		conn, err := GetConnection(c.opts.brokerAddr, c.opts.tlsConfig)
		if err == nil {
			c.conn = conn
			return nil
		}
		sleepDuration := utils.CalculateJitter(delay, c.opts.jitterPercent)
		log.Printf("CONSUMER - SUBSCRIBE ~> Failed connecting to %s (attempt %d/%d): %v, Retrying in %v...\n", c.opts.brokerAddr, i+1, c.opts.maxRetries, err, sleepDuration)
		time.Sleep(sleepDuration)
		delay *= 2
		if delay > c.opts.maxBackoff {
			delay = c.opts.maxBackoff
		}
	}

	return fmt.Errorf("could not connect to server %s after %d attempts: %w", c.opts.brokerAddr, c.opts.maxRetries, err)
}

func (c *Consumer) readMessage(ctx context.Context, conn net.Conn) error {
	msg, err := c.receive(ctx, conn)
	if err == nil {
		ctx = SetHeaders(ctx, msg.Headers)
		return c.OnMessage(ctx, msg, conn)
	}
	if err.Error() == "EOF" || strings.Contains(err.Error(), "closed network connection") {
		err1 := c.OnClose(ctx, conn)
		if err1 != nil {
			return err1
		}
		return err
	}
	c.OnError(ctx, conn, err)
	return err
}

func (c *Consumer) Consume(ctx context.Context) error {
	err := c.attemptConnect()
	if err != nil {
		return err
	}
	c.pool = NewPool(
		c.opts.numOfWorkers,
		WithTaskQueueSize(c.opts.queueSize),
		WithMaxMemoryLoad(c.opts.maxMemoryLoad),
		WithHandler(c.ProcessTask),
		WithPoolCallback(c.OnResponse),
		WithTaskStorage(c.opts.storage),
	)
	if err := c.subscribe(ctx, c.queue); err != nil {
		return fmt.Errorf("failed to connect to server for queue %s: %v", c.queue, err)
	}
	c.pool.Start(c.opts.numOfWorkers)
	if c.opts.enableHTTPApi {
		go func() {
			_, err := c.StartHTTPAPI()
			if err != nil {
				log.Println(fmt.Sprintf("Error on running HTTP API %s", err.Error()))
			}
		}()
	}
	// Infinite loop to continuously read messages and reconnect if needed.
	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping consumer.")
			return nil
		default:
			if c.opts.ConsumerRateLimiter != nil {
				c.opts.ConsumerRateLimiter.Wait()
			}
			if err := c.readMessage(ctx, c.conn); err != nil {
				log.Printf("Error reading message: %v, attempting reconnection...", err)
				for {
					if ctx.Err() != nil {
						return nil
					}
					if rErr := c.attemptConnect(); rErr != nil {
						log.Printf("Reconnection attempt failed: %v", rErr)
						time.Sleep(c.opts.initialDelay)
					} else {
						break
					}
				}
				if err := c.subscribe(ctx, c.queue); err != nil {
					log.Printf("Failed to re-subscribe on reconnection: %v", err)
					time.Sleep(c.opts.initialDelay)
				}
			}
		}
	}
}

func (c *Consumer) waitForAck(ctx context.Context, conn net.Conn) error {
	msg, err := c.receive(ctx, conn)
	if err != nil {
		return err
	}
	if msg.Command == consts.SUBSCRIBE_ACK {
		log.Printf("CONSUMER - SUBSCRIBE_ACK ~> %s on %s", c.id, msg.Queue)
		return nil
	}
	return fmt.Errorf("expected SUBSCRIBE_ACK, got: %v", msg.Command)
}

func (c *Consumer) Pause(ctx context.Context) error {
	return c.operate(ctx, consts.CONSUMER_PAUSED, c.pool.Pause)
}

func (c *Consumer) Update(ctx context.Context, payload []byte) error {
	var newConfig DynamicConfig
	if err := json.Unmarshal(payload, &newConfig); err != nil {
		log.Printf("Invalid payload for CONSUMER_UPDATE: %v", err)
		return err
	}
	if c.pool != nil {
		if err := c.pool.UpdateConfig(&newConfig); err != nil {
			log.Printf("Failed to update pool config: %v", err)
			return err
		}
	}
	return c.sendOpsMessage(ctx, consts.CONSUMER_UPDATED)
}

func (c *Consumer) Resume(ctx context.Context) error {
	return c.operate(ctx, consts.CONSUMER_RESUMED, c.pool.Resume)
}

func (c *Consumer) Stop(ctx context.Context) error {
	return c.operate(ctx, consts.CONSUMER_STOPPED, c.pool.Stop)
}

func (c *Consumer) operate(ctx context.Context, cmd consts.CMD, poolOperation func()) error {
	poolOperation()
	if err := c.sendOpsMessage(ctx, cmd); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) sendOpsMessage(ctx context.Context, cmd consts.CMD) error {
	headers := HeadersWithConsumerID(ctx, c.id)
	msg := codec.NewMessage(cmd, nil, c.queue, headers)
	return c.send(ctx, c.conn, msg)
}

func (c *Consumer) Conn() net.Conn {
	return c.conn
}

// StartHTTPAPI starts an HTTP server on a random available port and registers API endpoints.
// It returns the port number the server is listening on.
func (c *Consumer) StartHTTPAPI() (int, error) {
	// Listen on a random port.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, fmt.Errorf("failed to start listener: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	// Create a new HTTP mux and register endpoints.
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", c.handleStats)
	mux.HandleFunc("/update", c.handleUpdate)
	mux.HandleFunc("/pause", c.handlePause)
	mux.HandleFunc("/resume", c.handleResume)
	mux.HandleFunc("/stop", c.handleStop)

	// Start the server in a new goroutine.
	go func() {
		// Log errors if the HTTP server stops.
		if err := http.Serve(ln, mux); err != nil {
			log.Printf("HTTP server error on port %d: %v", port, err)
		}
	}()

	log.Printf("HTTP API for consumer %s started on port %d", c.id, port)
	return port, nil
}

// handleStats responds with JSON containing consumer and pool metrics.
func (c *Consumer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Gather consumer and pool stats using formatted metrics.
	stats := map[string]interface{}{
		"consumer_id":  c.id,
		"queue":        c.queue,
		"pool_metrics": c.pool.FormattedMetrics(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode stats: %v", err), http.StatusInternalServerError)
	}
}

// handleUpdate accepts a POST request with a JSON payload to update the consumer's pool configuration.
// It reuses the consumer's Update method which updates the pool configuration.
func (c *Consumer) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read the request body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Call the Update method on the consumer (which in turn updates the pool configuration).
	if err := c.Update(r.Context(), body); err != nil {
		http.Error(w, fmt.Sprintf("failed to update configuration: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{"status": "configuration updated"}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

// handlePause pauses the consumer's pool.
func (c *Consumer) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := c.Pause(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("failed to pause consumer: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{"status": "consumer paused"}
	json.NewEncoder(w).Encode(resp)
}

// handleResume resumes the consumer's pool.
func (c *Consumer) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := c.Resume(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("failed to resume consumer: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{"status": "consumer resumed"}
	json.NewEncoder(w).Encode(resp)
}

// handleStop stops the consumer's pool.
func (c *Consumer) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Stop the consumer.
	if err := c.Stop(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("failed to stop consumer: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{"status": "consumer stopped"}
	json.NewEncoder(w).Encode(resp)
}
