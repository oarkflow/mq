package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type TaskManager struct {
	dag         *DAG
	wg          sync.WaitGroup
	mutex       sync.Mutex
	results     []Result
	nodeResults map[string]Result
	done        chan struct{}
}

func NewTaskManager(d *DAG) *TaskManager {
	return &TaskManager{
		dag:         d,
		nodeResults: make(map[string]Result),
		results:     make([]Result, 0),
		done:        make(chan struct{}),
	}
}

func (tm *TaskManager) processTask(ctx context.Context, nodeID string, task *Task) Result {
	node, ok := tm.dag.Nodes[nodeID]
	if !ok {
		return Result{Error: fmt.Errorf("nodeID %s not found", nodeID)}
	}
	tm.wg.Add(1)
	go tm.processNode(ctx, node, task, nil)
	go func() {
		tm.wg.Wait()
		close(tm.done)
	}()
	select {
	case <-ctx.Done():
		return Result{Error: ctx.Err()}
	case <-tm.done:
		tm.mutex.Lock()
		defer tm.mutex.Unlock()
		if len(tm.results) == 1 {
			return tm.callback(tm.results[0])
		}
		return tm.callback(tm.results)
	}
}

func (tm *TaskManager) callback(results any) Result {
	var rs Result
	switch res := results.(type) {
	case []Result:
		aggregatedOutput := make([]json.RawMessage, 0)
		for i, result := range res {
			if i == 0 {
				rs.TaskID = result.TaskID
			}
			var item json.RawMessage
			err := json.Unmarshal(result.Payload, &item)
			if err != nil {
				rs.Error = err
				return rs
			}
			aggregatedOutput = append(aggregatedOutput, item)
		}
		finalOutput, err := json.Marshal(aggregatedOutput)
		if err != nil {
			rs.Error = err
			return rs
		}
		rs.Payload = finalOutput
	case Result:
		rs.TaskID = res.TaskID
		var item json.RawMessage
		err := json.Unmarshal(res.Payload, &item)
		if err != nil {
			rs.Error = err
			return rs
		}
		finalOutput, err := json.Marshal(item)
		if err != nil {
			rs.Error = err
			return rs
		}
		rs.Payload = finalOutput
	}
	return rs
}

func (tm *TaskManager) appendFinalResult(result Result) {
	tm.mutex.Lock()
	tm.results = append(tm.results, result)
	tm.nodeResults[result.NodeKey] = result
	tm.mutex.Unlock()
}

func (tm *TaskManager) processNode(ctx context.Context, node *Node, task *Task, parentNode *Node) {
	defer tm.wg.Done()
	var result Result
	select {
	case <-ctx.Done():
		result = Result{TaskID: task.ID, NodeKey: node.Key, Error: ctx.Err()}
		tm.appendFinalResult(result)
		return
	default:
		result = node.handler(ctx, task)
		if result.Error != nil {
			tm.appendFinalResult(result)
			return
		}
	}
	tm.mutex.Lock()
	task.Results[node.Key] = result
	tm.mutex.Unlock()

	edges := make([]Edge, len(node.Edges))
	copy(edges, node.Edges)
	if result.Status != "" {
		if conditions, ok := tm.dag.conditions[result.NodeKey]; ok {
			if targetNodeKey, ok := conditions[result.Status]; ok {
				if targetNode, ok := tm.dag.Nodes[targetNodeKey]; ok {
					edges = append(edges, Edge{
						From: node,
						To:   targetNode,
						Type: SimpleEdge,
					})
				}
			}
		}
	}
	if len(edges) == 0 {
		if parentNode != nil {
			tm.appendFinalResult(result)
		}
		return
	}
	for _, edge := range edges {
		switch edge.Type {
		case LoopEdge:
			var items []json.RawMessage
			err := json.Unmarshal(result.Payload, &items)
			if err != nil {
				tm.appendFinalResult(Result{TaskID: task.ID, NodeKey: node.Key, Error: err})
				return
			}
			for _, item := range items {
				loopTask := &Task{
					ID:      task.ID,
					NodeKey: edge.From.Key,
					Payload: item,
					Results: task.Results,
				}
				tm.wg.Add(1)
				go tm.processNode(ctx, edge.To, loopTask, node)
			}
		case SimpleEdge:
			if edge.To != nil {
				tm.wg.Add(1)
				t := &Task{
					ID:      task.ID,
					NodeKey: edge.From.Key,
					Payload: result.Payload,
					Results: task.Results,
				}
				go tm.processNode(ctx, edge.To, t, node)
			} else if parentNode != nil {
				tm.appendFinalResult(result)
			}
		}
	}
}
