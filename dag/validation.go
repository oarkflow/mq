package dag

import (
	"fmt"
)

// DAGValidator provides validation capabilities for DAG structure
type DAGValidator struct {
	dag *DAG
}

// NewDAGValidator creates a new DAG validator
func NewDAGValidator(dag *DAG) *DAGValidator {
	return &DAGValidator{dag: dag}
}

// ValidateStructure performs comprehensive DAG structure validation
func (v *DAGValidator) ValidateStructure() error {
	if err := v.validateCycles(); err != nil {
		return fmt.Errorf("cycle validation failed: %w", err)
	}

	if err := v.validateConnectivity(); err != nil {
		return fmt.Errorf("connectivity validation failed: %w", err)
	}

	if err := v.validateNodeTypes(); err != nil {
		return fmt.Errorf("node type validation failed: %w", err)
	}

	if err := v.validateStartNode(); err != nil {
		return fmt.Errorf("start node validation failed: %w", err)
	}

	return nil
}

// validateCycles detects cycles in the DAG using DFS
func (v *DAGValidator) validateCycles() error {
	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)

	var dfs func(nodeID string) error
	dfs = func(nodeID string) error {
		visited[nodeID] = true
		recursionStack[nodeID] = true

		node, exists := v.dag.nodes.Get(nodeID)
		if !exists {
			return fmt.Errorf("node %s not found", nodeID)
		}

		for _, edge := range node.Edges {
			if !visited[edge.To.ID] {
				if err := dfs(edge.To.ID); err != nil {
					return err
				}
			} else if recursionStack[edge.To.ID] {
				return fmt.Errorf("cycle detected: %s -> %s", nodeID, edge.To.ID)
			}
		}

		// Check conditional edges
		if conditions, exists := v.dag.conditions[nodeID]; exists {
			for _, targetNodeID := range conditions {
				if !visited[targetNodeID] {
					if err := dfs(targetNodeID); err != nil {
						return err
					}
				} else if recursionStack[targetNodeID] {
					return fmt.Errorf("cycle detected in condition: %s -> %s", nodeID, targetNodeID)
				}
			}
		}

		recursionStack[nodeID] = false
		return nil
	}

	// Check all nodes for cycles
	var nodeIDs []string
	v.dag.nodes.ForEach(func(id string, _ *Node) bool {
		nodeIDs = append(nodeIDs, id)
		return true
	})

	for _, nodeID := range nodeIDs {
		if !visited[nodeID] {
			if err := dfs(nodeID); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateConnectivity ensures all nodes are reachable
func (v *DAGValidator) validateConnectivity() error {
	if v.dag.startNode == "" {
		return fmt.Errorf("no start node defined")
	}

	reachable := make(map[string]bool)
	var dfs func(nodeID string)
	dfs = func(nodeID string) {
		if reachable[nodeID] {
			return
		}
		reachable[nodeID] = true

		node, exists := v.dag.nodes.Get(nodeID)
		if !exists {
			return
		}

		for _, edge := range node.Edges {
			dfs(edge.To.ID)
		}

		if conditions, exists := v.dag.conditions[nodeID]; exists {
			for _, targetNodeID := range conditions {
				dfs(targetNodeID)
			}
		}
	}

	dfs(v.dag.startNode)

	// Check for unreachable nodes
	var unreachableNodes []string
	v.dag.nodes.ForEach(func(id string, _ *Node) bool {
		if !reachable[id] {
			unreachableNodes = append(unreachableNodes, id)
		}
		return true
	})

	if len(unreachableNodes) > 0 {
		return fmt.Errorf("unreachable nodes detected: %v", unreachableNodes)
	}

	return nil
}

// validateNodeTypes ensures proper node type usage
func (v *DAGValidator) validateNodeTypes() error {
	pageNodeCount := 0

	v.dag.nodes.ForEach(func(id string, node *Node) bool {
		if node.NodeType == Page {
			pageNodeCount++
		}
		return true
	})

	if pageNodeCount > 1 {
		return fmt.Errorf("multiple page nodes detected, only one page node is allowed")
	}

	return nil
}

// validateStartNode ensures start node exists and is valid
func (v *DAGValidator) validateStartNode() error {
	if v.dag.startNode == "" {
		return fmt.Errorf("start node not specified")
	}

	if _, exists := v.dag.nodes.Get(v.dag.startNode); !exists {
		return fmt.Errorf("start node %s does not exist", v.dag.startNode)
	}

	return nil
}

// GetTopologicalOrder returns nodes in topological order
func (v *DAGValidator) GetTopologicalOrder() ([]string, error) {
	if err := v.validateCycles(); err != nil {
		return nil, err
	}

	inDegree := make(map[string]int)
	adjList := make(map[string][]string)

	// Initialize
	v.dag.nodes.ForEach(func(id string, _ *Node) bool {
		inDegree[id] = 0
		adjList[id] = []string{}
		return true
	})

	// Build adjacency list and calculate in-degrees
	v.dag.nodes.ForEach(func(id string, node *Node) bool {
		for _, edge := range node.Edges {
			adjList[id] = append(adjList[id], edge.To.ID)
			inDegree[edge.To.ID]++
		}

		if conditions, exists := v.dag.conditions[id]; exists {
			for _, targetNodeID := range conditions {
				adjList[id] = append(adjList[id], targetNodeID)
				inDegree[targetNodeID]++
			}
		}
		return true
	})

	// Kahn's algorithm for topological sorting
	queue := []string{}
	for nodeID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeID)
		}
	}

	var result []string
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		for _, neighbor := range adjList[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(result) != len(inDegree) {
		return nil, fmt.Errorf("cycle detected during topological sort")
	}

	return result, nil
}

// GetNodeStatistics returns DAG statistics
func (v *DAGValidator) GetNodeStatistics() map[string]any {
	stats := make(map[string]any)

	nodeCount := 0
	edgeCount := 0
	pageNodeCount := 0
	functionNodeCount := 0

	v.dag.nodes.ForEach(func(id string, node *Node) bool {
		nodeCount++
		edgeCount += len(node.Edges)

		if node.NodeType == Page {
			pageNodeCount++
		} else {
			functionNodeCount++
		}
		return true
	})

	conditionCount := len(v.dag.conditions)

	stats["total_nodes"] = nodeCount
	stats["total_edges"] = edgeCount
	stats["page_nodes"] = pageNodeCount
	stats["function_nodes"] = functionNodeCount
	stats["conditional_edges"] = conditionCount
	stats["start_node"] = v.dag.startNode

	return stats
}

// GetCriticalPath finds the longest path in the DAG
func (v *DAGValidator) GetCriticalPath() ([]string, error) {
	topOrder, err := v.GetTopologicalOrder()
	if err != nil {
		return nil, err
	}

	dist := make(map[string]int)
	parent := make(map[string]string)

	// Initialize distances
	v.dag.nodes.ForEach(func(id string, _ *Node) bool {
		dist[id] = -1
		return true
	})

	if v.dag.startNode != "" {
		dist[v.dag.startNode] = 0
	}

	// Process nodes in topological order
	for _, nodeID := range topOrder {
		if dist[nodeID] == -1 {
			continue
		}

		node, exists := v.dag.nodes.Get(nodeID)
		if !exists {
			continue
		}

		// Process direct edges
		for _, edge := range node.Edges {
			if dist[edge.To.ID] < dist[nodeID]+1 {
				dist[edge.To.ID] = dist[nodeID] + 1
				parent[edge.To.ID] = nodeID
			}
		}

		// Process conditional edges
		if conditions, exists := v.dag.conditions[nodeID]; exists {
			for _, targetNodeID := range conditions {
				if dist[targetNodeID] < dist[nodeID]+1 {
					dist[targetNodeID] = dist[nodeID] + 1
					parent[targetNodeID] = nodeID
				}
			}
		}
	}

	// Find the node with maximum distance
	maxDist := -1
	var endNode string
	for nodeID, d := range dist {
		if d > maxDist {
			maxDist = d
			endNode = nodeID
		}
	}

	if maxDist == -1 {
		return []string{}, nil
	}

	// Reconstruct path
	var path []string
	current := endNode
	for current != "" {
		path = append([]string{current}, path...)
		current = parent[current]
	}

	return path, nil
}
