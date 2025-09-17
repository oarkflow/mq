package dag

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

func (tm *DAG) SetStartNode(node string) {
	tm.startNode = node
}

func (tm *DAG) GetStartNode() string {
	return tm.startNode
}

func (tm *DAG) AddCondition(fromNode string, conditions map[string]string) *DAG {
	tm.conditions[fromNode] = conditions
	return tm
}

func (tm *DAG) AddNode(nodeType NodeType, name, nodeID string, handler mq.Processor, startNode ...bool) *DAG {
	if tm.Error != nil {
		return tm
	}

	// Configure consumer options based on node type
	consumerOpts := []mq.Option{mq.WithBrokerURL(tm.server.Options().BrokerAddr())}

	// Page nodes should have no timeout to allow unlimited time for user input
	if nodeType == Page {
		consumerOpts = append(consumerOpts, mq.WithConsumerTimeout(0)) // 0 = no timeout
	}

	con := mq.NewConsumer(nodeID, nodeID, handler.ProcessTask, consumerOpts...)
	n := &Node{
		Label:     name,
		ID:        nodeID,
		NodeType:  nodeType,
		processor: con,
	}
	if tm.server != nil && tm.server.SyncMode() {
		n.isReady = true
	}
	tm.nodes.Set(nodeID, n)
	if len(startNode) > 0 && startNode[0] {
		tm.startNode = nodeID
	}
	if nodeType == Page && !tm.hasPageNode {
		tm.hasPageNode = true
	}
	return tm
}

// AddNodeWithDebug adds a node to the DAG with optional debug mode enabled
func (tm *DAG) AddNodeWithDebug(nodeType NodeType, name, nodeID string, handler mq.Processor, debug bool, startNode ...bool) *DAG {
	dag := tm.AddNode(nodeType, name, nodeID, handler, startNode...)
	if dag.Error == nil {
		dag.SetNodeDebug(nodeID, debug)
	}
	return dag
}

func (tm *DAG) AddDeferredNode(nodeType NodeType, name, key string, firstNode ...bool) error {
	if tm.server.SyncMode() {
		return fmt.Errorf("DAG cannot have deferred node in Sync Mode")
	}
	tm.nodes.Set(key, &Node{
		Label:    name,
		ID:       key,
		NodeType: nodeType,
	})
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return nil
}

func (tm *DAG) IsReady() bool {
	var isReady bool
	tm.nodes.ForEach(func(_ string, n *Node) bool {
		if !n.isReady {
			return false
		}
		isReady = true
		return true
	})
	return isReady
}

func (tm *DAG) resolveNode(nodeID string) (*Node, bool) {
	nodeParts := strings.Split(nodeID, ".")
	if len(nodeParts) > 1 {
		nodeID = nodeParts[0]
	}
	return tm.nodes.Get(nodeID)
}

func (tm *DAG) AddEdge(edgeType EdgeType, label, from string, targets ...string) *DAG {
	if tm.Error != nil {
		return tm
	}
	if edgeType == Iterator {
		tm.iteratorNodes.Set(from, []Edge{})
	}
	node, ok := tm.resolveNode(from)
	if !ok {
		tm.Error = fmt.Errorf("node not found %s", from)
		return tm
	}
	for _, target := range targets {
		if targetNode, ok := tm.nodes.Get(target); ok {
			edge := Edge{From: node, To: targetNode, Type: edgeType, Label: label, FromSource: from}
			node.Edges = append(node.Edges, edge)
			if edgeType != Iterator {
				if edges, ok := tm.iteratorNodes.Get(node.ID); ok {
					edges = append(edges, edge)
					tm.iteratorNodes.Set(node.ID, edges)
				}
			}
		}
	}
	return tm
}

func (tm *DAG) getCurrentNode(manager *TaskManager) string {
	if manager.currentNodePayload.Size() == 0 {
		return ""
	}
	return manager.currentNodePayload.Keys()[0]
}

func (tm *DAG) AddDAGNode(nodeType NodeType, name string, key string, dag *DAG, firstNode ...bool) *DAG {
	dag.AssignTopic(key)
	dag.name += fmt.Sprintf("(%s)", name)
	tm.nodes.Set(key, &Node{
		Label:     name,
		ID:        key,
		NodeType:  nodeType,
		processor: dag,
		isReady:   true,
	})
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return tm
}

// RemoveNode removes the node with the given nodeID and adjusts the edges.
// For example, if A -> B and B -> C exist and B is removed, a new edge A -> C is created.
func (tm *DAG) RemoveNode(nodeID string) error {
	node, exists := tm.nodes.Get(nodeID)
	if !exists {
		return fmt.Errorf("node %s does not exist", nodeID)
	}
	// Collect incoming edges (from nodes pointing to the removed node).
	var incomingEdges []Edge
	tm.nodes.ForEach(func(_ string, n *Node) bool {
		for _, edge := range n.Edges {
			if edge.To.ID == nodeID {
				incomingEdges = append(incomingEdges, Edge{
					From:  n,
					To:    node,
					Label: edge.Label,
					Type:  edge.Type,
				})
			}
		}
		return true
	})
	// Get outgoing edges from the node being removed.
	outgoingEdges := node.Edges
	// For each incoming edge and each outgoing edge, create a new edge A -> C.
	for _, inEdge := range incomingEdges {
		for _, outEdge := range outgoingEdges {
			// Avoid creating self-loop.
			if inEdge.From.ID != outEdge.To.ID {
				newEdge := Edge{
					From:  inEdge.From,
					To:    outEdge.To,
					Label: inEdge.Label + "_" + outEdge.Label,
					Type:  Simple, // Use Simple edge type for adjusted flows.
				}
				// Append new edge if one doesn't already exist.
				for _, e := range inEdge.From.Edges {
					if e.To.ID == newEdge.To.ID {
						goto SKIP_ADD
					}
				}
				inEdge.From.Edges = append(inEdge.From.Edges, newEdge)
			}
		SKIP_ADD:
		}
	}
	// Remove all edges that are connected to the removed node.
	tm.nodes.ForEach(func(_ string, n *Node) bool {
		var updatedEdges []Edge
		for _, edge := range n.Edges {
			if edge.To.ID != nodeID {
				updatedEdges = append(updatedEdges, edge)
			}
		}
		n.Edges = updatedEdges
		return true
	})
	// Remove any conditions referencing the removed node.
	for key, cond := range tm.conditions {
		if key == nodeID {
			delete(tm.conditions, key)
		} else {
			for when, target := range cond {
				if target == nodeID {
					delete(cond, when)
				}
			}
		}
	}
	// Remove the node from the DAG.
	tm.nodes.Del(nodeID)
	// Invalidate caches.
	tm.nextNodesCache = nil
	tm.prevNodesCache = nil
	tm.Logger().Info("Node removed and edges adjusted",
		logger.Field{Key: "removed_node", Value: nodeID})
	return nil
}

// getOrCreateCircuitBreaker gets or creates a circuit breaker for a node
func (tm *DAG) getOrCreateCircuitBreaker(nodeID string) *CircuitBreaker {
	tm.circuitBreakersMu.RLock()
	cb, exists := tm.circuitBreakers[nodeID]
	tm.circuitBreakersMu.RUnlock()

	if exists {
		return cb
	}

	tm.circuitBreakersMu.Lock()
	defer tm.circuitBreakersMu.Unlock()

	// Double-check after acquiring write lock
	if cb, exists := tm.circuitBreakers[nodeID]; exists {
		return cb
	}

	// Create new circuit breaker with default config
	config := &CircuitBreakerConfig{
		FailureThreshold: 5,
		ResetTimeout:     30 * time.Second,
		HalfOpenMaxCalls: 3,
	}

	cb = NewCircuitBreaker(config, tm.Logger())
	tm.circuitBreakers[nodeID] = cb

	return cb
}

// Complete missing methods for DAG

func (tm *DAG) GetLastNodes() ([]*Node, error) {
	var lastNodes []*Node
	tm.nodes.ForEach(func(key string, node *Node) bool {
		if len(node.Edges) == 0 {
			if conds, exists := tm.conditions[node.ID]; !exists || len(conds) == 0 {
				lastNodes = append(lastNodes, node)
			}
		}
		return true
	})
	return lastNodes, nil
}

// parseInitialNode extracts the initial node from context
func (tm *DAG) parseInitialNode(ctx context.Context) (string, error) {
	if initialNode, ok := ctx.Value("initial_node").(string); ok && initialNode != "" {
		return initialNode, nil
	}

	// If no initial node specified, use start node
	if tm.startNode != "" {
		return tm.startNode, nil
	}

	// Find first node if no start node is set
	firstNode := tm.findStartNode()
	if firstNode != nil {
		return firstNode.ID, nil
	}

	return "", fmt.Errorf("no initial node found")
}

// findStartNode finds the first node in the DAG
func (tm *DAG) findStartNode() *Node {
	incomingEdges := make(map[string]bool)
	connectedNodes := make(map[string]bool)
	for _, node := range tm.nodes.AsMap() {
		for _, edge := range node.Edges {
			if edge.Type.IsValid() {
				connectedNodes[node.ID] = true
				connectedNodes[edge.To.ID] = true
				incomingEdges[edge.To.ID] = true
			}
		}
		if cond, ok := tm.conditions[node.ID]; ok {
			for _, target := range cond {
				connectedNodes[target] = true
				incomingEdges[target] = true
			}
		}
	}
	for nodeID, node := range tm.nodes.AsMap() {
		if !incomingEdges[nodeID] && connectedNodes[nodeID] {
			return node
		}
	}
	return nil
}

// IsLastNode checks if a node is the last node in the DAG
func (tm *DAG) IsLastNode(nodeID string) (bool, error) {
	node, exists := tm.nodes.Get(nodeID)
	if !exists {
		return false, fmt.Errorf("node %s not found", nodeID)
	}

	// Check if node has any outgoing edges
	if len(node.Edges) > 0 {
		return false, nil
	}

	// Check if node has any conditional edges
	if conditions, exists := tm.conditions[nodeID]; exists && len(conditions) > 0 {
		return false, nil
	}

	return true, nil
}

// GetNextNodes returns the next nodes for a given node
func (tm *DAG) GetNextNodes(nodeID string) ([]*Node, error) {
	nodeID = strings.Split(nodeID, Delimiter)[0]
	if tm.nextNodesCache != nil {
		if cached, exists := tm.nextNodesCache[nodeID]; exists {
			return cached, nil
		}
	}

	node, exists := tm.nodes.Get(nodeID)
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	var nextNodes []*Node

	// Add direct edge targets
	for _, edge := range node.Edges {
		nextNodes = append(nextNodes, edge.To)
	}

	// Add conditional targets
	if conditions, exists := tm.conditions[nodeID]; exists {
		for _, targetID := range conditions {
			if targetNode, ok := tm.nodes.Get(targetID); ok {
				nextNodes = append(nextNodes, targetNode)
			}
		}
	}

	// Cache the result
	if tm.nextNodesCache != nil {
		tm.nextNodesCache[nodeID] = nextNodes
	}

	return nextNodes, nil
}

// GetPreviousNodes returns the previous nodes for a given node
func (tm *DAG) GetPreviousNodes(nodeID string) ([]*Node, error) {
	nodeID = strings.Split(nodeID, Delimiter)[0]
	if tm.prevNodesCache != nil {
		if cached, exists := tm.prevNodesCache[nodeID]; exists {
			return cached, nil
		}
	}

	var prevNodes []*Node

	// Find nodes that point to this node
	tm.nodes.ForEach(func(id string, node *Node) bool {
		// Check direct edges
		for _, edge := range node.Edges {
			if edge.To.ID == nodeID {
				prevNodes = append(prevNodes, node)
				break
			}
		}

		// Check conditional edges
		if conditions, exists := tm.conditions[id]; exists {
			for _, targetID := range conditions {
				if targetID == nodeID {
					prevNodes = append(prevNodes, node)
					break
				}
			}
		}

		return true
	})

	// Cache the result
	if tm.prevNodesCache != nil {
		tm.prevNodesCache[nodeID] = prevNodes
	}

	return prevNodes, nil
}

// GetNodeByID returns a node by its ID
func (tm *DAG) GetNodeByID(nodeID string) (*Node, error) {
	node, exists := tm.nodes.Get(nodeID)
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return node, nil
}

// GetAllNodes returns all nodes in the DAG
func (tm *DAG) GetAllNodes() map[string]*Node {
	result := make(map[string]*Node)
	tm.nodes.ForEach(func(id string, node *Node) bool {
		result[id] = node
		return true
	})
	return result
}

// GetNodeCount returns the total number of nodes
func (tm *DAG) GetNodeCount() int {
	return tm.nodes.Size()
}
