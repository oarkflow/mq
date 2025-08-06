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
			fmt.Println(strings.Join(c, ", "))
		}
		var edges []string
		for _, target := range node.Edges {
			edges = append(edges, fmt.Sprintf("%s (%s)", target.To.Label, target.To.ID))
		}
		fmt.Println(strings.Join(edges, ", "))
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

// ExportDOT generates a clean, professional DOT graph representation
func (tm *DAG) ExportDOT(direction ...Direction) string {
	rankDir := TB
	if len(direction) > 0 && direction[0] != "" {
		rankDir = direction[0]
	}

	var sb strings.Builder

	// Clean, professional graph styling
	sb.WriteString(fmt.Sprintf(`digraph "%s" {`, tm.name))
	sb.WriteString("\n")
	sb.WriteString(`  graph [`)
	sb.WriteString(`rankdir=` + string(rankDir) + `, `)
	sb.WriteString(`bgcolor="#F8F9FA", `)
	sb.WriteString(`fontname="Arial", `)
	sb.WriteString(`fontsize=14, `)
	sb.WriteString(`labelloc="t", `)
	sb.WriteString(`pad=0.5`)
	sb.WriteString(`];`)
	sb.WriteString("\n")

	// Node defaults
	sb.WriteString(`  node [`)
	sb.WriteString(`fontname="Arial", `)
	sb.WriteString(`fontsize=11, `)
	sb.WriteString(`style="filled,rounded", `)
	sb.WriteString(`penwidth=2`)
	sb.WriteString(`];`)
	sb.WriteString("\n")

	// Edge defaults
	sb.WriteString(`  edge [`)
	sb.WriteString(`fontname="Arial", `)
	sb.WriteString(`fontsize=10, `)
	sb.WriteString(`arrowsize=0.8`)
	sb.WriteString(`];`)
	sb.WriteString("\n\n")

	// Graph title
	sb.WriteString(fmt.Sprintf(`  label="%s";`, tm.name))
	sb.WriteString("\n\n")

	// Render the DAG properly
	tm.renderCleanDAG(&sb, "  ")

	sb.WriteString("}\n")
	return sb.String()
}

// renderCleanDAG renders the DAG with proper sub-DAG integration
func (tm *DAG) renderCleanDAG(sb *strings.Builder, indent string) {
	sortedNodes := tm.TopologicalSort()

	// Step 1: Render all main DAG nodes (including sub-DAG representative nodes)
	sb.WriteString(fmt.Sprintf("%s// Main DAG Nodes\n", indent))
	for _, nodeID := range sortedNodes {
		node, _ := tm.nodes.Get(nodeID)
		if !tm.isSubDAGNode(node) {
			tm.renderCleanNode(sb, node, indent)
		}
	}
	sb.WriteString("\n")

	// Step 2: Render sub-DAG clusters (internal structure only)
	sb.WriteString(fmt.Sprintf("%s// Sub-DAG Internal Structures\n", indent))
	for _, nodeID := range sortedNodes {
		node, _ := tm.nodes.Get(nodeID)
		if subDAG, ok := isDAGNode(node); ok && subDAG.consumerTopic != "" {
			tm.renderSubDAGCluster(sb, nodeID, subDAG, indent)
		}
	}
	sb.WriteString("\n")

	// Step 3: Render all edges (main DAG connections)
	sb.WriteString(fmt.Sprintf("%s// Main DAG Edges\n", indent))
	for _, nodeID := range sortedNodes {
		node, _ := tm.nodes.Get(nodeID)
		tm.renderCleanEdges(sb, node, indent)
	}
	sb.WriteString("\n")

	// Step 4: Render conditional edges
	sb.WriteString(fmt.Sprintf("%s// Conditional Edges\n", indent))
	tm.renderCleanConditionalEdges(sb, indent)
}

// renderCleanNode renders a single node with appropriate styling
func (tm *DAG) renderCleanNode(sb *strings.Builder, node *Node, indent string) {
	var color, shape, icon string

	// Check if this is a sub-DAG node
	if subDAG, ok := isDAGNode(node); ok && subDAG.consumerTopic != "" {
		color = "#E8F6F3"
		shape = "box"
		icon = "🔄"
	} else {
		switch node.NodeType {
		case Function:
			color = "#E8F6F3"
			shape = "box"
			icon = "⚙️"
		case Page:
			color = "#FEF9E7"
			shape = "note"
			icon = "📄"
		default:
			color = "#EBF5FB"
			shape = "ellipse"
			icon = "🔄"
		}
	}

	label := fmt.Sprintf("%s %s", icon, node.Label)

	sb.WriteString(fmt.Sprintf("%s\"%s\" [", indent, node.ID))
	sb.WriteString(fmt.Sprintf(`label="%s", `, label))
	sb.WriteString(fmt.Sprintf(`fillcolor="%s", `, color))
	sb.WriteString(fmt.Sprintf(`shape=%s`, shape))
	sb.WriteString("];\n")
}

// renderSubDAGCluster renders the internal structure of a sub-DAG
func (tm *DAG) renderSubDAGCluster(sb *strings.Builder, parentNodeID string, subDAG *DAG, indent string) {
	clusterName := fmt.Sprintf("cluster_%s", parentNodeID)

	sb.WriteString(fmt.Sprintf("%ssubgraph \"%s\" {\n", indent, clusterName))
	sb.WriteString(fmt.Sprintf("%s  label=\"Internal: %s\";\n", indent, subDAG.name))
	sb.WriteString(fmt.Sprintf("%s  style=\"dashed\";\n", indent))
	sb.WriteString(fmt.Sprintf("%s  color=\"#3498DB\";\n", indent))
	sb.WriteString(fmt.Sprintf("%s  fontsize=10;\n", indent))
	sb.WriteString("\n")

	// Render sub-DAG nodes with prefix
	subSortedNodes := subDAG.TopologicalSort()
	for _, subNodeID := range subSortedNodes {
		subNode, _ := subDAG.nodes.Get(subNodeID)
		prefixedID := fmt.Sprintf("%s_%s", parentNodeID, subNodeID)
		tm.renderPrefixedNode(sb, subNode, prefixedID, indent+"  ")
	}

	// Render sub-DAG edges
	for _, subNodeID := range subSortedNodes {
		subNode, _ := subDAG.nodes.Get(subNodeID)
		tm.renderPrefixedEdges(sb, subNode, parentNodeID, indent+"  ")
	}

	// Render sub-DAG conditional edges
	for fromNodeID, conditions := range subDAG.conditions {
		for condition, toNodeID := range conditions {
			fromPrefixed := fmt.Sprintf("%s_%s", parentNodeID, fromNodeID)
			toPrefixed := fmt.Sprintf("%s_%s", parentNodeID, toNodeID)
			sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\" [", indent+"  ", fromPrefixed, toPrefixed))
			sb.WriteString(fmt.Sprintf(`label="[%s]", `, condition))
			sb.WriteString(`color="#8E44AD", `)
			sb.WriteString(`style=dashed`)
			sb.WriteString("];\n")
		}
	}

	sb.WriteString(fmt.Sprintf("%s}\n", indent))
	sb.WriteString("\n")
}

// renderPrefixedNode renders a node with a prefix (for sub-DAG nodes)
func (tm *DAG) renderPrefixedNode(sb *strings.Builder, node *Node, prefixedID, indent string) {
	var color, shape, icon string

	switch node.NodeType {
	case Function:
		color = "#D5E8D4"
		shape = "box"
		icon = "⚙️"
	case Page:
		color = "#FFE6CC"
		shape = "note"
		icon = "📄"
	default:
		color = "#DAE8FC"
		shape = "ellipse"
		icon = "🔄"
	}

	label := fmt.Sprintf("%s %s", icon, node.Label)

	sb.WriteString(fmt.Sprintf("%s\"%s\" [", indent, prefixedID))
	sb.WriteString(fmt.Sprintf(`label="%s", `, label))
	sb.WriteString(fmt.Sprintf(`fillcolor="%s", `, color))
	sb.WriteString(fmt.Sprintf(`shape=%s, `, shape))
	sb.WriteString(`fontsize=9`)
	sb.WriteString("];\n")
}

// renderCleanEdges renders edges for a node
func (tm *DAG) renderCleanEdges(sb *strings.Builder, node *Node, indent string) {
	for _, edge := range node.Edges {
		from := strings.Join(strings.Split(edge.FromSource, "."), "_")
		sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\"", indent, from, edge.To.ID))
		if edge.Label != "" {
			sb.WriteString(fmt.Sprintf(` [label="%s"]`, edge.Label))
		}
		sb.WriteString(";\n")
	}
}

// renderPrefixedEdges renders edges with prefixes (for sub-DAG internal edges)
func (tm *DAG) renderPrefixedEdges(sb *strings.Builder, node *Node, prefix, indent string) {
	fromPrefixed := fmt.Sprintf("%s_%s", prefix, node.ID)
	for _, edge := range node.Edges {
		toPrefixed := fmt.Sprintf("%s_%s", prefix, edge.To.ID)
		sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\"", indent, fromPrefixed, toPrefixed))
		if edge.Label != "" {
			sb.WriteString(fmt.Sprintf(` [label="%s"]`, edge.Label))
		}
		sb.WriteString(";\n")
	}
}

// renderCleanConditionalEdges renders conditional edges
func (tm *DAG) renderCleanConditionalEdges(sb *strings.Builder, indent string) {
	for fromNodeID, conditions := range tm.conditions {
		for condition, toNodeID := range conditions {
			sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\" [", indent, fromNodeID, toNodeID))
			sb.WriteString(fmt.Sprintf(`label="[%s]", `, condition))
			sb.WriteString(`color="#8E44AD", `)
			sb.WriteString(`style=dashed`)
			sb.WriteString("];\n")
		}
	}
}

// renderAllConditionalEdges renders all conditional edges from main DAG
func (tm *DAG) renderAllConditionalEdges(sb *strings.Builder, prefix, indent string, sortedNodes []string) {
	if len(tm.conditions) > 0 {
		for fromNodeKey, conditions := range tm.conditions {
			for when, then := range conditions {
				if toNode, ok := tm.nodes.Get(then); ok {
					tm.renderConditionalEdge(sb, fromNodeKey, toNode.ID, when, prefix, indent)
				}
			}
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

// renderConditionalEdge renders a single conditional edge with enhanced styling
func (tm *DAG) renderConditionalEdge(sb *strings.Builder, fromNodeKey, toNodeKey, condition, prefix, indent string) {
	fromID := fmt.Sprintf("%s%s", prefix, fromNodeKey)
	toID := fmt.Sprintf("%s%s", prefix, toNodeKey)

	sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\" [\n", indent, fromID, toID))
	sb.WriteString(fmt.Sprintf("%s  label=\"🔀 [%s]\",\n", indent, condition))
	sb.WriteString(fmt.Sprintf("%s  color=\"#8E44AD\",\n", indent))
	sb.WriteString(fmt.Sprintf("%s  style=\"dashed,bold\",\n", indent))
	sb.WriteString(fmt.Sprintf("%s  fontsize=11,\n", indent))
	sb.WriteString(fmt.Sprintf("%s  fontcolor=\"#8E44AD\",\n", indent))
	sb.WriteString(fmt.Sprintf("%s  arrowsize=1.0,\n", indent))
	sb.WriteString(fmt.Sprintf("%s  penwidth=2.5,\n", indent))
	sb.WriteString(fmt.Sprintf("%s  constraint=true\n", indent))
	sb.WriteString(fmt.Sprintf("%s];\n", indent))
}

// renderNode creates a professional node representation with enhanced styling
func renderNode(sb *strings.Builder, node *Node, indent string, prefix ...string) {
	prefixedID := fmt.Sprintf("%s%s", strings.Join(prefix, ""), node.ID)

	// Enhanced node styling based on type
	var (
		nodeFill    string
		borderColor string
		shape       string
		icon        string
		labelStyle  string
	)

	switch node.NodeType {
	case Function:
		nodeFill = "#E8F8F5"    // Light mint green
		borderColor = "#27AE60" // Green border
		shape = "box"
		icon = "⚙️"
		labelStyle = "bold"
	case Page:
		nodeFill = "#FEF9E7"    // Light yellow
		borderColor = "#F39C12" // Orange border
		shape = "note"
		icon = "📄"
		labelStyle = "bold"
	default:
		nodeFill = "#EBF5FB"    // Light blue
		borderColor = "#3498DB" // Blue border
		shape = "ellipse"
		icon = "🔄"
		labelStyle = "normal"
	}

	// Create enhanced label with icon and metadata
	enhancedLabel := fmt.Sprintf("%s %s\\n(%s)", icon, node.Label, node.ID)

	// Apply comprehensive styling with better spacing
	sb.WriteString(fmt.Sprintf("%s\"%s\" [\n", indent, prefixedID))
	sb.WriteString(fmt.Sprintf("%s  label=\"%s\",\n", indent, enhancedLabel))
	sb.WriteString(fmt.Sprintf("%s  shape=%s,\n", indent, shape))
	sb.WriteString(fmt.Sprintf("%s  style=\"filled,rounded,%s\",\n", indent, labelStyle))
	sb.WriteString(fmt.Sprintf("%s  fillcolor=\"%s\",\n", indent, nodeFill))
	sb.WriteString(fmt.Sprintf("%s  color=\"%s\",\n", indent, borderColor))
	sb.WriteString(fmt.Sprintf("%s  fontcolor=\"#2C3E50\",\n", indent))
	sb.WriteString(fmt.Sprintf("%s  penwidth=2.5,\n", indent))
	sb.WriteString(fmt.Sprintf("%s  margin=0.6,\n", indent)) // Increased margin
	sb.WriteString(fmt.Sprintf("%s  width=2.0,\n", indent))  // Minimum width
	sb.WriteString(fmt.Sprintf("%s  height=1.0,\n", indent)) // Minimum height
	sb.WriteString(fmt.Sprintf("%s  tooltip=\"%s: %s\"\n", indent, node.NodeType, node.Label))
	sb.WriteString(fmt.Sprintf("%s];\n", indent))
}

func (tm *DAG) TopologicalSort() (stack []string) {
	visited := make(map[string]bool)
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		if !visited[node.ID] {
			tm.topologicalSortUtil(node.ID, visited, &stack)
		}
		return true
	})
	for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
		stack[i], stack[j] = stack[j], stack[i]
	}
	return
}

func (tm *DAG) topologicalSortUtil(v string, visited map[string]bool, stack *[]string) {
	visited[v] = true
	node, ok := tm.nodes.Get(v)
	if !ok {
		fmt.Println("Not found", v, tm.key)
	}
	for _, edge := range node.Edges {
		if !visited[edge.To.ID] {
			tm.topologicalSortUtil(edge.To.ID, visited, stack)
		}
	}
	*stack = append(*stack, v)
}

func isDAGNode(node *Node) (*DAG, bool) {
	switch node := node.processor.(type) {
	case *DAG:
		return node, true
	default:
		return nil, false
	}
}
