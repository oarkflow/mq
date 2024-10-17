package mq

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

type TaskStorage interface {
	SaveTask(task *QueueTask) error
	GetTask(taskID string) (*QueueTask, error)
	DeleteTask(taskID string) error
	GetAllTasks() ([]*QueueTask, error)
	FetchNextTask() (*QueueTask, error)
	CleanupExpiredTasks() error
}

type MemoryTaskStorage struct {
	tasks      PriorityQueue
	taskLock   sync.Mutex
	expiryTime time.Duration
}

func NewMemoryTaskStorage(expiryTime time.Duration) *MemoryTaskStorage {
	return &MemoryTaskStorage{
		tasks:      make(PriorityQueue, 0),
		expiryTime: expiryTime,
	}
}

func (m *MemoryTaskStorage) SaveTask(task *QueueTask) error {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()
	heap.Push(&m.tasks, task)
	return nil
}

func (m *MemoryTaskStorage) GetTask(taskID string) (*QueueTask, error) {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()
	for _, task := range m.tasks {
		if task.payload.ID == taskID {
			return task, nil
		}
	}
	return nil, fmt.Errorf("task not found")
}

func (m *MemoryTaskStorage) DeleteTask(taskID string) error {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()
	for i, task := range m.tasks {
		if task.payload.ID == taskID {
			heap.Remove(&m.tasks, i)
			return nil
		}
	}
	return fmt.Errorf("task not found")
}

func (m *MemoryTaskStorage) GetAllTasks() ([]*QueueTask, error) {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()
	tasks := make([]*QueueTask, len(m.tasks))
	for i, task := range m.tasks {
		tasks[i] = task
	}
	return tasks, nil
}

func (m *MemoryTaskStorage) FetchNextTask() (*QueueTask, error) {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()
	if len(m.tasks) == 0 {
		return nil, fmt.Errorf("no tasks available")
	}

	task := heap.Pop(&m.tasks).(*QueueTask)
	if task.payload.CreatedAt.Add(m.expiryTime).Before(time.Now()) {
		m.DeleteTask(task.payload.ID)
		return m.FetchNextTask()
	}
	return task, nil
}

func (m *MemoryTaskStorage) CleanupExpiredTasks() error {
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	for i := 0; i < len(m.tasks); i++ {
		task := m.tasks[i]
		if task.payload.CreatedAt.Add(m.expiryTime).Before(time.Now()) {
			heap.Remove(&m.tasks, i)
			i-- // Adjust index after removal
		}
	}
	return nil
}

/*
type PostgresTaskStorage struct {
	db *sql.DB
}

func NewPostgresTaskStorage(db *sql.DB) *PostgresTaskStorage {
	return &PostgresTaskStorage{db: db}
}

func (p *PostgresTaskStorage) SaveTask(task *QueueTask) error {
	query := `
        INSERT INTO tasks (id, payload, priority, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (id) DO NOTHING`
	payloadBytes, err := utils.Serialize(task.payload) // Serialize converts the task to bytes
	if err != nil {
		return err
	}
	_, err = p.db.Exec(query, task.payload.ID, payloadBytes, task.priority, task.payload.CreatedAt, time.Now())
	return err
}

func (p *PostgresTaskStorage) GetTask(taskID string) (*QueueTask, error) {
	query := `SELECT id, payload, priority, created_at FROM tasks WHERE id = $1`
	var task QueueTask
	var payloadBytes []byte
	err := p.db.QueryRow(query, taskID).Scan(&task.payload.ID, &payloadBytes, &task.priority, &task.payload.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("task not found")
		}
		return nil, err
	}

	task.payload, err = utils.Deserialize(payloadBytes) // Deserialize converts bytes to Task object
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (p *PostgresTaskStorage) DeleteTask(taskID string) error {
	query := `DELETE FROM tasks WHERE id = $1`
	_, err := p.db.Exec(query, taskID)
	return err
}

func (p *PostgresTaskStorage) GetAllTasks() ([]*QueueTask, error) {
	query := `SELECT id, payload, priority, created_at FROM tasks`
	rows, err := p.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*QueueTask
	for rows.Next() {
		var task QueueTask
		var payloadBytes []byte
		err := rows.Scan(&task.payload.ID, &payloadBytes, &task.priority, &task.payload.CreatedAt)
		if err != nil {
			return nil, err
		}

		task.payload, err = utils.Deserialize(payloadBytes)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func (p *PostgresTaskStorage) FetchNextTask() (*QueueTask, error) {
	query := `
        SELECT id, payload, priority, created_at FROM tasks
        ORDER BY priority DESC, created_at ASC
        LIMIT 1`

	var task QueueTask
	var payloadBytes []byte
	err := p.db.QueryRow(query).Scan(&task.payload.ID, &payloadBytes, &task.priority, &task.payload.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("no tasks available")
		}
		return nil, err
	}

	task.payload, err = utils.Deserialize(payloadBytes)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (p *PostgresTaskStorage) CleanupExpiredTasks() error {
	query := `DELETE FROM tasks WHERE created_at < $1`
	_, err := p.db.Exec(query, time.Now().Add(-time.Hour*24)) // Assuming tasks older than 24 hours are expired
	return err
}
*/
