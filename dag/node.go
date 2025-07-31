package dag

import (
	"context"
	"fmt"
	"strings"
)

func (tm *DAG) GetNextNodes(key string) ([]*Node, error) {
	key = strings.Split(key, Delimiter)[0]
	// use cache if available
	if tm.nextNodesCache != nil {
		if next, ok := tm.nextNodesCache[key]; ok {
			return next, nil
		}
	}
	node, exists := tm.nodes.Get(key)
	if !exists {
		return nil, fmt.Errorf("Node with key %s does not exist while getting next node", key)
	}
	var successors []*Node
	for _, edge := range node.Edges {
		successors = append(successors, edge.To)
	}
	if conds, exists := tm.conditions[key]; exists {
		for _, targetKey := range conds {
			if targetNode, exists := tm.nodes.Get(targetKey); exists {
				successors = append(successors, targetNode)
			}
		}
	}
	return successors, nil
}

func (tm *DAG) GetPreviousNodes(key string) ([]*Node, error) {
	key = strings.Split(key, Delimiter)[0]
	// use cache if available
	if tm.prevNodesCache != nil {
		if prev, ok := tm.prevNodesCache[key]; ok {
			return prev, nil
		}
	}
	var predecessors []*Node
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		for _, target := range node.Edges {
			if target.To.ID == key {
				predecessors = append(predecessors, node)
			}
		}
		return true
	})
	for fromNode, conds := range tm.conditions {
		for _, targetKey := range conds {
			if targetKey == key {
				node, exists := tm.nodes.Get(fromNode)
				if !exists {
					return nil, fmt.Errorf("Node with key %s does not exist while getting previous node", fromNode)
				}
				predecessors = append(predecessors, node)
			}
		}
	}
	return predecessors, nil
}

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

func (tm *DAG) IsLastNode(key string) (bool, error) {
	node, exists := tm.nodes.Get(key)
	if !exists {
		return false, fmt.Errorf("Node with key %s does not exist", key)
	}
	if len(node.Edges) > 0 {
		return false, nil
	}
	if conds, exists := tm.conditions[node.ID]; exists && len(conds) > 0 {
		return false, nil
	}
	return true, nil
}

func (tm *DAG) parseInitialNode(ctx context.Context) (string, error) {
	val := ctx.Value("initial_node")
	initialNode, ok := val.(string)
	if ok {
		return initialNode, nil
	}
	if tm.startNode == "" {
		firstNode := tm.findStartNode()
		if firstNode != nil {
			tm.startNode = firstNode.ID
		}
	}

	if tm.startNode == "" {
		return "", fmt.Errorf("initial node not found")
	}
	return tm.startNode, nil
}

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
