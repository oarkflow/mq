package dag

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func (tm *DAG) PrintGraph() {
	fmt.Println("DAG Graph structure:")
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		if conditions, ok := tm.conditions[node.ID]; ok {
			var c []string
			for when, then := range conditions {
				if target, ok := tm.nodes.Get(then); ok {
					c = append(c, fmt.Sprintf("If [%s] Then %s (%s)", when, target.Label, target.ID))
				}
			}
			if len(c) > 0 {
				fmt.Printf("  %s (%s): %s\n", node.Label, node.ID, strings.Join(c, ", "))
			}
		}
		var edges []string
		for _, target := range node.Edges {
			edges = append(edges, fmt.Sprintf("%s (%s)", target.To.Label, target.To.ID))
		}
		if len(edges) > 0 {
			fmt.Printf("  %s (%s) → %s\n", node.Label, node.ID, strings.Join(edges, ", "))
		}
		return true
	})
}

func (tm *DAG) ClassifyEdges(startNodes ...string) (string, bool, error) {
	builder := &strings.Builder{}
	startNode := tm.GetStartNode()
	if len(startNodes) > 0 && startNodes[0] != "" {
		startNode = startNodes[0]
	}
	visited := make(map[string]bool)
	discoveryTime := make(map[string]int)
	finishedTime := make(map[string]int)
	timeVal := 0
	inRecursionStack := make(map[string]bool)
	if startNode == "" {
		firstNode := tm.findStartNode()
		if firstNode != nil {
			startNode = firstNode.ID
		}
	}
	if startNode == "" {
		return "", false, fmt.Errorf("no start node found")
	}
	hasCycle, cycleErr := tm.dfs(startNode, visited, discoveryTime, finishedTime, &timeVal, inRecursionStack, builder)
	if cycleErr != nil {
		return builder.String(), hasCycle, cycleErr
	}
	return builder.String(), hasCycle, nil
}

func (tm *DAG) dfs(v string, visited map[string]bool, discoveryTime, finishedTime map[string]int, timeVal *int, inRecursionStack map[string]bool, builder *strings.Builder) (bool, error) {
	visited[v] = true
	inRecursionStack[v] = true
	*timeVal++
	discoveryTime[v] = *timeVal
	node, _ := tm.nodes.Get(v)
	hasCycle := false
	var err error
	for _, edge := range node.Edges {
		if !visited[edge.To.ID] {
			builder.WriteString(fmt.Sprintf("Traversing Edge: %s -> %s\n", v, edge.To.ID))
			hasCycle, err := tm.dfs(edge.To.ID, visited, discoveryTime, finishedTime, timeVal, inRecursionStack, builder)
			if err != nil {
				return true, err
			}
			if hasCycle {
				return true, nil
			}
		} else if inRecursionStack[edge.To.ID] {
			cycleMsg := fmt.Sprintf("Cycle detected: %s -> %s\n", v, edge.To.ID)
			return true, fmt.Errorf("%s", cycleMsg)
		}
	}
	hasCycle, err = tm.handleConditionalEdges(v, visited, discoveryTime, finishedTime, timeVal, inRecursionStack, builder)
	if err != nil {
		return true, err
	}
	*timeVal++
	finishedTime[v] = *timeVal
	inRecursionStack[v] = false
	return hasCycle, nil
}

func (tm *DAG) handleConditionalEdges(v string, visited map[string]bool, discoveryTime, finishedTime map[string]int, time *int, inRecursionStack map[string]bool, builder *strings.Builder) (bool, error) {
	node, _ := tm.nodes.Get(v)
	for when, then := range tm.conditions[node.ID] {
		if targetNode, ok := tm.nodes.Get(then); ok {
			if !visited[targetNode.ID] {
				builder.WriteString(fmt.Sprintf("Traversing Conditional Edge [%s]: %s -> %s\n", when, v, targetNode.ID))
				hasCycle, err := tm.dfs(targetNode.ID, visited, discoveryTime, finishedTime, time, inRecursionStack, builder)
				if err != nil {
					return true, err
				}
				if hasCycle {
					return true, nil
				}
			} else if inRecursionStack[targetNode.ID] {
				cycleMsg := fmt.Sprintf("Cycle detected in Conditional Edge [%s]: %s -> %s\n", when, v, targetNode.ID)
				return true, fmt.Errorf("%s", cycleMsg)
			}
		}
	}
	return false, nil
}

func (tm *DAG) SaveDOTFile(filename string, direction ...Direction) error {
	dotContent := tm.ExportDOT(direction...)
	return os.WriteFile(filename, []byte(dotContent), 0644)
}

func (tm *DAG) SaveSVG(svgFile string) error {
	return tm.saveImage(svgFile, "-Tsvg")
}

func (tm *DAG) SavePNG(pngFile string) error {
	return tm.saveImage(pngFile, "-Tpng")
}

func (tm *DAG) saveImage(fileName string, arg string) error {
	dotFile := fileName[:len(fileName)-4] + ".dot"
	if err := tm.SaveDOTFile(dotFile); err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(dotFile)
	}()
	cmd := exec.Command("dot", arg, dotFile, "-o", fileName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert image: %w", err)
	}
	return nil
}

// ExportDOT generates a clean, compact DOT graph with proper spacing
func (tm *DAG) ExportDOT(direction ...Direction) string {
	rankDir := TB
	if len(direction) > 0 && direction[0] != "" {
		rankDir = direction[0]
	}

	var sb strings.Builder

	// Compact graph with proper spacing
	sb.WriteString(fmt.Sprintf(`digraph "%s" {`, tm.name))
	sb.WriteString("\n")

	// Optimized graph attributes for compact, clean layout
	sb.WriteString(`  graph [`)
	sb.WriteString(`rankdir=` + string(rankDir) + `, `)
	sb.WriteString(`bgcolor="white", `)
	sb.WriteString(`pad="0.2", `)
	sb.WriteString(`nodesep="0.3", `)
	sb.WriteString(`ranksep="0.5", `)
	sb.WriteString(`splines="ortho", `)
	sb.WriteString(`concentrate="true", `)
	sb.WriteString(`overlap="false", `)
	sb.WriteString(`packmode="graph"`)
	sb.WriteString(`];`)
	sb.WriteString("\n")

	// Compact node styling
	sb.WriteString(`  node [`)
	sb.WriteString(`fontname="Arial", `)
	sb.WriteString(`fontsize=10, `)
	sb.WriteString(`style="filled,rounded", `)
	sb.WriteString(`penwidth=1, `)
	sb.WriteString(`margin="0.1,0.05", `)
	sb.WriteString(`width="0", `)
	sb.WriteString(`height="0", `)
	sb.WriteString(`fixedsize="false"`)
	sb.WriteString(`];`)
	sb.WriteString("\n")

	// Clean edge styling
	sb.WriteString(`  edge [`)
	sb.WriteString(`fontname="Arial", `)
	sb.WriteString(`fontsize=8, `)
	sb.WriteString(`color="#666666", `)
	sb.WriteString(`penwidth=1.5, `)
	sb.WriteString(`arrowsize=0.7`)
	sb.WriteString(`];`)
	sb.WriteString("\n\n")

	// Render the DAG with compact styling
	tm.renderCompactDAG(&sb, "  ")

	sb.WriteString("}\n")
	return sb.String()
}

// renderCompactDAG renders the DAG with clean, compact styling
func (tm *DAG) renderCompactDAG(sb *strings.Builder, indent string) {
	sortedNodes := tm.TopologicalSort()

	// Render nodes with compact styling
	for _, nodeID := range sortedNodes {
		node, exists := tm.nodes.Get(nodeID)
		if !exists {
			continue
		}
		if !tm.isSubDAGNode(node) {
			tm.renderCompactNode(sb, node, indent)
		}
	}
	sb.WriteString("\n")

	// Render sub-DAG clusters if any
	hasSubDAGs := false
	for _, nodeID := range sortedNodes {
		node, exists := tm.nodes.Get(nodeID)
		if !exists {
			continue
		}
		if subDAG, ok := isDAGNode(node); ok && subDAG.consumerTopic != "" {
			if !hasSubDAGs {
				sb.WriteString(fmt.Sprintf("%s// Sub-workflows\n", indent))
				hasSubDAGs = true
			}
			tm.renderCompactSubDAGCluster(sb, nodeID, subDAG, indent)
		}
	}
	if hasSubDAGs {
		sb.WriteString("\n")
	}

	// Render regular edges
	for _, nodeID := range sortedNodes {
		node, exists := tm.nodes.Get(nodeID)
		if !exists {
			continue
		}
		tm.renderCompactEdges(sb, node, indent)
	}

	// Render conditional edges if any
	if len(tm.conditions) > 0 {
		sb.WriteString("\n")
		tm.renderCompactConditionalEdges(sb, indent)
	}
}

// renderCompactNode renders a single node with clean, compact styling
func (tm *DAG) renderCompactNode(sb *strings.Builder, node *Node, indent string) {
	var fillColor, shape string

	// Check if this is a sub-DAG node
	if subDAG, ok := isDAGNode(node); ok && subDAG.consumerTopic != "" {
		fillColor = "#e1f5fe"
		shape = "box"
	} else {
		switch node.NodeType {
		case Function:
			fillColor = "#f1f8e9"
			shape = "box"
		case Page:
			fillColor = "#fff3e0"
			shape = "box"
		default:
			fillColor = "#f5f5f5"
			shape = "ellipse"
		}
	}

	// Clean, simple label
	cleanLabel := strings.ReplaceAll(node.Label, `"`, `\"`)

	sb.WriteString(fmt.Sprintf("%s\"%s\" [", indent, node.ID))
	sb.WriteString(fmt.Sprintf(`label="%s", `, cleanLabel))
	sb.WriteString(fmt.Sprintf(`fillcolor="%s", `, fillColor))
	sb.WriteString(fmt.Sprintf(`shape=%s`, shape))
	sb.WriteString("];\n")
}

// renderCompactSubDAGCluster renders sub-DAG with clean cluster styling
func (tm *DAG) renderCompactSubDAGCluster(sb *strings.Builder, parentNodeID string, subDAG *DAG, indent string) {
	clusterName := fmt.Sprintf("cluster_%s", parentNodeID)

	sb.WriteString(fmt.Sprintf("%ssubgraph \"%s\" {\n", indent, clusterName))
	sb.WriteString(fmt.Sprintf("%s  label=\"%s\";\n", indent, subDAG.name))
	sb.WriteString(fmt.Sprintf("%s  style=\"dashed\";\n", indent))
	sb.WriteString(fmt.Sprintf("%s  color=\"#999999\";\n", indent))
	sb.WriteString(fmt.Sprintf("%s  fontsize=9;\n", indent))

	// Render sub-DAG nodes compactly
	subSortedNodes := subDAG.TopologicalSort()
	for _, subNodeID := range subSortedNodes {
		subNode, exists := subDAG.nodes.Get(subNodeID)
		if !exists {
			continue
		}
		prefixedID := fmt.Sprintf("%s_%s", parentNodeID, subNodeID)
		tm.renderCompactPrefixedNode(sb, subNode, prefixedID, indent+"  ")
	}

	// Render sub-DAG edges
	for _, subNodeID := range subSortedNodes {
		subNode, exists := subDAG.nodes.Get(subNodeID)
		if !exists {
			continue
		}
		tm.renderCompactPrefixedEdges(sb, subNode, parentNodeID, indent+"  ")
	}

	// Render sub-DAG conditional edges
	for fromNodeID, conditions := range subDAG.conditions {
		for condition, toNodeID := range conditions {
			fromPrefixed := fmt.Sprintf("%s_%s", parentNodeID, fromNodeID)
			toPrefixed := fmt.Sprintf("%s_%s", parentNodeID, toNodeID)
			cleanCondition := strings.ReplaceAll(condition, `"`, `\"`)
			sb.WriteString(fmt.Sprintf("%s  \"%s\" -> \"%s\" [", indent, fromPrefixed, toPrefixed))
			sb.WriteString(fmt.Sprintf(`label="%s", `, cleanCondition))
			sb.WriteString(`style="dashed", `)
			sb.WriteString(`color="#ff9800"`)
			sb.WriteString("];\n")
		}
	}

	sb.WriteString(fmt.Sprintf("%s}\n", indent))
}

// renderCompactPrefixedNode renders a prefixed node with compact styling
func (tm *DAG) renderCompactPrefixedNode(sb *strings.Builder, node *Node, prefixedID, indent string) {
	var fillColor, shape string

	switch node.NodeType {
	case Function:
		fillColor = "#e8f5e8"
		shape = "box"
	case Page:
		fillColor = "#ffeaa7"
		shape = "box"
	default:
		fillColor = "#f0f0f0"
		shape = "ellipse"
	}

	cleanLabel := strings.ReplaceAll(node.Label, `"`, `\"`)

	sb.WriteString(fmt.Sprintf("%s\"%s\" [", indent, prefixedID))
	sb.WriteString(fmt.Sprintf(`label="%s", `, cleanLabel))
	sb.WriteString(fmt.Sprintf(`fillcolor="%s", `, fillColor))
	sb.WriteString(fmt.Sprintf(`shape=%s, `, shape))
	sb.WriteString(`fontsize=9`)
	sb.WriteString("];\n")
}

// renderCompactEdges renders edges with clean styling
func (tm *DAG) renderCompactEdges(sb *strings.Builder, node *Node, indent string) {
	for _, edge := range node.Edges {
		fromID := strings.Join(strings.Split(edge.FromSource, "."), "_")
		sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\"", indent, fromID, edge.To.ID))

		if edge.Label != "" {
			cleanLabel := strings.ReplaceAll(edge.Label, `"`, `\"`)
			sb.WriteString(fmt.Sprintf(` [label="%s"]`, cleanLabel))
		}
		sb.WriteString(";\n")
	}
}

// renderCompactPrefixedEdges renders prefixed edges with clean styling
func (tm *DAG) renderCompactPrefixedEdges(sb *strings.Builder, node *Node, prefix, indent string) {
	fromPrefixed := fmt.Sprintf("%s_%s", prefix, node.ID)
	for _, edge := range node.Edges {
		toPrefixed := fmt.Sprintf("%s_%s", prefix, edge.To.ID)
		sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\"", indent, fromPrefixed, toPrefixed))

		if edge.Label != "" {
			cleanLabel := strings.ReplaceAll(edge.Label, `"`, `\"`)
			sb.WriteString(fmt.Sprintf(` [label="%s"]`, cleanLabel))
		}
		sb.WriteString(";\n")
	}
}

// renderCompactConditionalEdges renders conditional edges with clean styling
func (tm *DAG) renderCompactConditionalEdges(sb *strings.Builder, indent string) {
	for fromNodeID, conditions := range tm.conditions {
		for condition, toNodeID := range conditions {
			cleanCondition := strings.ReplaceAll(condition, `"`, `\"`)
			sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\" [", indent, fromNodeID, toNodeID))
			sb.WriteString(fmt.Sprintf(`label="%s", `, cleanCondition))
			sb.WriteString(`style="dashed", `)
			sb.WriteString(`color="#ff9800"`)
			sb.WriteString("];\n")
		}
	}
}

// isSubDAGNode checks if a node contains a sub-DAG
func (tm *DAG) isSubDAGNode(node *Node) bool {
	if node.processor == nil {
		return false
	}
	_, ok := isDAGNode(node)
	return ok
}

func (tm *DAG) TopologicalSort() (stack []string) {
	visited := make(map[string]bool)
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		if !visited[node.ID] {
			tm.topologicalSortUtil(node.ID, visited, &stack)
		}
		return true
	})
	// Reverse the stack to get correct topological order
	for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
		stack[i], stack[j] = stack[j], stack[i]
	}
	return
}

func (tm *DAG) topologicalSortUtil(v string, visited map[string]bool, stack *[]string) {
	visited[v] = true
	node, ok := tm.nodes.Get(v)
	if !ok {
		fmt.Printf("Warning: Node not found: %s in DAG: %s\n", v, tm.key)
		return
	}
	for _, edge := range node.Edges {
		if !visited[edge.To.ID] {
			tm.topologicalSortUtil(edge.To.ID, visited, stack)
		}
	}
	*stack = append(*stack, v)
}

func isDAGNode(node *Node) (*DAG, bool) {
	switch processor := node.processor.(type) {
	case *DAG:
		return processor, true
	default:
		return nil, false
	}
}
