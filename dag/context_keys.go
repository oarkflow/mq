// context_keys.go
package dag

type contextKey string

const (
	contextKeyTaskID      contextKey = "task_id"
	contextKeyMethod      contextKey = "method"
	contextKeyInitialNode contextKey = "initial_node"
)
