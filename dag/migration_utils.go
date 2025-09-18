package dag

import (
	"fmt"
	"time"
)

// MigrationUtility provides utilities to convert existing DAG configurations to workflow definitions
type MigrationUtility struct {
	dag *DAG
}

// NewMigrationUtility creates a new migration utility
func NewMigrationUtility(dag *DAG) *MigrationUtility {
	return &MigrationUtility{
		dag: dag,
	}
}

// ConvertDAGToWorkflow converts an existing DAG to a workflow definition
func (m *MigrationUtility) ConvertDAGToWorkflow(workflowID, workflowName, version string) (*WorkflowDefinition, error) {
	if m.dag == nil {
		return nil, fmt.Errorf("DAG is nil")
	}

	workflow := &WorkflowDefinition{
		ID:          workflowID,
		Name:        workflowName,
		Description: fmt.Sprintf("Migrated from DAG: %s", m.dag.name),
		Version:     version,
		Status:      WorkflowStatusActive,
		Tags:        []string{"migrated", "dag"},
		Category:    "migrated",
		Owner:       "system",
		Nodes:       []WorkflowNode{},
		Edges:       []WorkflowEdge{},
		Variables:   make(map[string]Variable),
		Config: WorkflowConfig{
			Priority:      PriorityMedium,
			Concurrency:   1,
			EnableAudit:   true,
			EnableMetrics: true,
		},
		Metadata:  make(map[string]interface{}),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		CreatedBy: "migration-utility",
		UpdatedBy: "migration-utility",
	}

	// Convert DAG nodes to workflow nodes
	nodeMap := make(map[string]bool) // Track processed nodes

	m.dag.nodes.ForEach(func(nodeID string, node *Node) bool {
		workflowNode := m.convertDAGNodeToWorkflowNode(node)
		workflow.Nodes = append(workflow.Nodes, workflowNode)
		nodeMap[nodeID] = true
		return true
	})

	// Convert DAG edges to workflow edges
	edgeID := 1
	m.dag.nodes.ForEach(func(nodeID string, node *Node) bool {
		for _, edge := range node.Edges {
			workflowEdge := WorkflowEdge{
				ID:       fmt.Sprintf("edge_%d", edgeID),
				FromNode: edge.From.ID,
				ToNode:   edge.To.ID,
				Label:    edge.Label,
				Priority: 1,
				Metadata: make(map[string]interface{}),
			}

			// Add condition for conditional edges
			if edge.Type == Iterator {
				workflowEdge.Condition = "iterator_condition"
				workflowEdge.Metadata["original_type"] = "iterator"
			}

			workflow.Edges = append(workflow.Edges, workflowEdge)
			edgeID++
		}
		return true
	})

	// Add metadata about the original DAG
	workflow.Metadata["original_dag_name"] = m.dag.name
	workflow.Metadata["original_dag_key"] = m.dag.key
	workflow.Metadata["migration_timestamp"] = time.Now()
	workflow.Metadata["migration_version"] = "1.0"

	return workflow, nil
}

// convertDAGNodeToWorkflowNode converts a DAG node to a workflow node
func (m *MigrationUtility) convertDAGNodeToWorkflowNode(dagNode *Node) WorkflowNode {
	workflowNode := WorkflowNode{
		ID:          dagNode.ID,
		Name:        dagNode.Label,
		Description: fmt.Sprintf("Migrated DAG node: %s", dagNode.Label),
		Position: Position{
			X: 0, // Default position - will need to be set by UI
			Y: 0,
		},
		Metadata: make(map[string]interface{}),
	}

	// Convert node type
	workflowNode.Type = m.convertDAGNodeType(dagNode.NodeType)

	// Set timeout if specified
	if dagNode.Timeout > 0 {
		workflowNode.Timeout = &dagNode.Timeout
	}

	// Create basic configuration
	workflowNode.Config = WorkflowNodeConfig{
		Variables: make(map[string]string),
		Custom:    make(map[string]interface{}),
	}

	// Add original DAG node information to metadata
	workflowNode.Metadata["original_node_type"] = dagNode.NodeType.String()
	workflowNode.Metadata["is_ready"] = dagNode.isReady
	workflowNode.Metadata["debug"] = dagNode.Debug
	workflowNode.Metadata["is_first"] = dagNode.IsFirst
	workflowNode.Metadata["is_last"] = dagNode.IsLast

	// Set default retry policy
	workflowNode.RetryPolicy = &RetryPolicy{
		MaxAttempts: 3,
		BackoffMs:   1000,
		Jitter:      true,
		Timeout:     time.Minute * 5,
	}

	return workflowNode
}

// convertDAGNodeType converts DAG node type to workflow node type
func (m *MigrationUtility) convertDAGNodeType(dagNodeType NodeType) WorkflowNodeType {
	switch dagNodeType {
	case Function:
		return WorkflowNodeTypeTask
	case Page:
		return WorkflowNodeTypeHTML
	default:
		return WorkflowNodeTypeTask
	}
}

// ConvertWorkflowToDAG converts a workflow definition back to DAG structure
func (m *MigrationUtility) ConvertWorkflowToDAG(workflow *WorkflowDefinition) (*DAG, error) {
	// Create new DAG
	dag := NewDAG(workflow.Name, workflow.ID, nil)

	// Convert workflow nodes to DAG nodes
	for _, workflowNode := range workflow.Nodes {
		dagNode := m.convertWorkflowNodeToDAGNode(&workflowNode)
		dag.nodes.Set(dagNode.ID, dagNode)
	}

	// Convert workflow edges to DAG edges
	for _, workflowEdge := range workflow.Edges {
		fromNode, fromExists := dag.nodes.Get(workflowEdge.FromNode)
		toNode, toExists := dag.nodes.Get(workflowEdge.ToNode)

		if !fromExists || !toExists {
			continue
		}

		edge := Edge{
			From:       fromNode,
			FromSource: workflowEdge.FromNode,
			To:         toNode,
			Label:      workflowEdge.Label,
			Type:       m.convertWorkflowEdgeType(workflowEdge),
		}

		fromNode.Edges = append(fromNode.Edges, edge)
	}

	return dag, nil
}

// convertWorkflowNodeToDAGNode converts a workflow node to a DAG node
func (m *MigrationUtility) convertWorkflowNodeToDAGNode(workflowNode *WorkflowNode) *Node {
	dagNode := &Node{
		ID:       workflowNode.ID,
		Label:    workflowNode.Name,
		NodeType: m.convertWorkflowNodeTypeToDAG(workflowNode.Type),
		Edges:    []Edge{},
		isReady:  true,
	}

	// Set timeout if specified
	if workflowNode.Timeout != nil {
		dagNode.Timeout = *workflowNode.Timeout
	}

	// Extract metadata
	if workflowNode.Metadata != nil {
		if debug, ok := workflowNode.Metadata["debug"].(bool); ok {
			dagNode.Debug = debug
		}
		if isFirst, ok := workflowNode.Metadata["is_first"].(bool); ok {
			dagNode.IsFirst = isFirst
		}
		if isLast, ok := workflowNode.Metadata["is_last"].(bool); ok {
			dagNode.IsLast = isLast
		}
	}

	// Create a basic processor (this would need to be enhanced based on node type)
	dagNode.processor = &workflowNodeProcessor{
		node: workflowNode,
	}

	return dagNode
}

// convertWorkflowNodeTypeToDAG converts workflow node type to DAG node type
func (m *MigrationUtility) convertWorkflowNodeTypeToDAG(workflowNodeType WorkflowNodeType) NodeType {
	switch workflowNodeType {
	case WorkflowNodeTypeHTML:
		return Page
	case WorkflowNodeTypeTask:
		return Function
	default:
		return Function
	}
}

// convertWorkflowEdgeType converts workflow edge to DAG edge type
func (m *MigrationUtility) convertWorkflowEdgeType(workflowEdge WorkflowEdge) EdgeType {
	// Check metadata for original type
	if workflowEdge.Metadata != nil {
		if originalType, ok := workflowEdge.Metadata["original_type"].(string); ok {
			if originalType == "iterator" {
				return Iterator
			}
		}
	}

	// Check for conditions to determine edge type
	if workflowEdge.Condition != "" {
		return Iterator
	}

	return Simple
}

// ValidateWorkflowDefinition validates a workflow definition for common issues
func (m *MigrationUtility) ValidateWorkflowDefinition(workflow *WorkflowDefinition) []string {
	var issues []string

	// Check required fields
	if workflow.ID == "" {
		issues = append(issues, "Workflow ID is required")
	}
	if workflow.Name == "" {
		issues = append(issues, "Workflow name is required")
	}
	if workflow.Version == "" {
		issues = append(issues, "Workflow version is required")
	}

	// Check nodes
	if len(workflow.Nodes) == 0 {
		issues = append(issues, "Workflow must have at least one node")
	}

	// Check for duplicate node IDs
	nodeIDs := make(map[string]bool)
	for _, node := range workflow.Nodes {
		if node.ID == "" {
			issues = append(issues, "Node ID is required")
			continue
		}
		if nodeIDs[node.ID] {
			issues = append(issues, fmt.Sprintf("Duplicate node ID: %s", node.ID))
		}
		nodeIDs[node.ID] = true
	}

	// Validate edges
	for _, edge := range workflow.Edges {
		if !nodeIDs[edge.FromNode] {
			issues = append(issues, fmt.Sprintf("Edge references non-existent from node: %s", edge.FromNode))
		}
		if !nodeIDs[edge.ToNode] {
			issues = append(issues, fmt.Sprintf("Edge references non-existent to node: %s", edge.ToNode))
		}
	}

	// Check for cycles (simplified check)
	if m.hasSimpleCycle(workflow) {
		issues = append(issues, "Workflow contains cycles which may cause infinite loops")
	}

	return issues
}

// hasSimpleCycle performs a simple cycle detection
func (m *MigrationUtility) hasSimpleCycle(workflow *WorkflowDefinition) bool {
	// Build adjacency list
	adj := make(map[string][]string)
	for _, edge := range workflow.Edges {
		adj[edge.FromNode] = append(adj[edge.FromNode], edge.ToNode)
	}

	// Track visited nodes
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	// Check each node for cycles
	for _, node := range workflow.Nodes {
		if !visited[node.ID] {
			if m.hasCycleDFS(node.ID, adj, visited, recStack) {
				return true
			}
		}
	}

	return false
}

// hasCycleDFS performs DFS-based cycle detection
func (m *MigrationUtility) hasCycleDFS(nodeID string, adj map[string][]string, visited, recStack map[string]bool) bool {
	visited[nodeID] = true
	recStack[nodeID] = true

	// Visit all adjacent nodes
	for _, neighbor := range adj[nodeID] {
		if !visited[neighbor] {
			if m.hasCycleDFS(neighbor, adj, visited, recStack) {
				return true
			}
		} else if recStack[neighbor] {
			return true
		}
	}

	recStack[nodeID] = false
	return false
}

// GenerateWorkflowTemplate creates a basic workflow template
func (m *MigrationUtility) GenerateWorkflowTemplate(name, id string) *WorkflowDefinition {
	return &WorkflowDefinition{
		ID:          id,
		Name:        name,
		Description: "Generated workflow template",
		Version:     "1.0.0",
		Status:      WorkflowStatusDraft,
		Tags:        []string{"template"},
		Category:    "template",
		Owner:       "system",
		Nodes: []WorkflowNode{
			{
				ID:          "start_node",
				Name:        "Start",
				Type:        WorkflowNodeTypeTask,
				Description: "Starting node",
				Position:    Position{X: 100, Y: 100},
				Config: WorkflowNodeConfig{
					Script: "echo 'Workflow started'",
				},
			},
			{
				ID:          "end_node",
				Name:        "End",
				Type:        WorkflowNodeTypeTask,
				Description: "Ending node",
				Position:    Position{X: 300, Y: 100},
				Config: WorkflowNodeConfig{
					Script: "echo 'Workflow completed'",
				},
			},
		},
		Edges: []WorkflowEdge{
			{
				ID:       "edge_1",
				FromNode: "start_node",
				ToNode:   "end_node",
				Label:    "Proceed",
				Priority: 1,
			},
		},
		Variables: make(map[string]Variable),
		Config: WorkflowConfig{
			Priority:      PriorityMedium,
			Concurrency:   1,
			EnableAudit:   true,
			EnableMetrics: true,
		},
		Metadata:  make(map[string]interface{}),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		CreatedBy: "migration-utility",
		UpdatedBy: "migration-utility",
	}
}
