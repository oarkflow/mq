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
		fmt.Printf("Node: %s (%s) -> ", node.Label, node.ID)
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
			return true, fmt.Errorf(cycleMsg)
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
				return true, fmt.Errorf(cycleMsg)
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

// Refactored ExportDOT for a modern, enterprise look.
func (tm *DAG) ExportDOT(direction ...Direction) string {
	rankDir := TB
	if len(direction) > 0 && direction[0] != "" {
		rankDir = direction[0]
	}
	var sb strings.Builder
	// Graph properties with a clean background and smooth layout
	sb.WriteString(fmt.Sprintf(`digraph "%s" {`, tm.name))
	sb.WriteString("\n")
	sb.WriteString(`  graph [layout=dot, splines=polyline, overlap=false, bgcolor="#FAFAFA", fontname="Helvetica", fontsize=12];`)
	sb.WriteString("\n")
	// Nodes get a sophisticated gradient fill, drop shadow effect simulated via penwidth and color, and rounded borders.
	sb.WriteString(`  node [shape=box, style="filled,rounded", gradientangle=135, fontname="Helvetica", fontsize=10, penwidth=2, color="#2C3E50", fillcolor="#FFFFFF"];`)
	sb.WriteString("\n")
	// Edges with smooth curves and subtle colors.
	sb.WriteString(`  edge [fontname="Helvetica", fontsize=9, color="#7F8C8D", arrowsize=0.8, style=solid];`)
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf(`  rankdir=%s;`, rankDir))
	sb.WriteString("\n")
	// Render nodes with advanced styling
	sortedNodes := tm.TopologicalSort()
	for _, nodeKey := range sortedNodes {
		node, _ := tm.nodes.Get(nodeKey)
		renderNode(&sb, node, "  ")
	}
	// Render normal edges with enhanced styling.
	for _, nodeKey := range sortedNodes {
		node, _ := tm.nodes.Get(nodeKey)
		renderEdges(&sb, node, "  ")
	}
	// Render subgraphs for sub-DAGs with a distinct dashed border.
	for _, nodeKey := range sortedNodes {
		node, _ := tm.nodes.Get(nodeKey)
		if node.processor != nil {
			if subDAG, ok := isDAGNode(node); ok && subDAG.consumerTopic != "" {
				prefix := subDAG.name + "_"
				sb.WriteString(fmt.Sprintf(`  subgraph "cluster_%s" {`+"\n", subDAG.name))
				sb.WriteString(fmt.Sprintf("    label = \"Sub-DAG: %s\";\n", subDAG.name))
				sb.WriteString("    style = dashed;\n")
				sb.WriteString("    color = \"#A6ACAF\";\n")
				for _, subNodeKey := range subDAG.TopologicalSort() {
					subNode, _ := subDAG.nodes.Get(subNodeKey)
					renderNode(&sb, subNode, "    ", prefix)
				}
				for _, subNodeKey := range subDAG.TopologicalSort() {
					subNode, _ := subDAG.nodes.Get(subNodeKey)
					renderEdges(&sb, subNode, "    ", prefix)
				}
				sb.WriteString("  }\n")
				if startNodeKey := subDAG.TopologicalSort()[0]; startNodeKey != "" {
					// Connect parent node to sub-DAG start node using correct prefix
					sb.WriteString(fmt.Sprintf("  \"%s\" -> \"%s%s\" [label=\"Subconnect\", color=\"#16A085\", style=bold, fontsize=10];\n", nodeKey, prefix, startNodeKey))
				}
			}
		}
	}
	// Render conditional edges with dotted style.
	for fromNodeKey, conditions := range tm.conditions {
		for when, then := range conditions {
			if toNode, ok := tm.nodes.Get(then); ok {
				sb.WriteString(fmt.Sprintf("  \"%s\" -> \"%s\" [label=\"%s\", color=\"#8E44AD\", style=dotted, fontsize=9, arrowsize=0.6];\n", fromNodeKey, toNode.ID, when))
			}
		}
	}
	sb.WriteString("}\n")
	return sb.String()
}

// Enhanced renderNode with a modern professional style.
func renderNode(sb *strings.Builder, node *Node, indent string, prefix ...string) {
	prefixedID := fmt.Sprintf("%s%s", strings.Join(prefix, ""), node.ID)
	labelSuffix := ""
	nodeFill := "#F0F3F4" // Default light tone
	switch node.NodeType {
	case Function:
		nodeFill = "#D4EFDF" // soft green
		labelSuffix = " ƒ(x)"
	case Page:
		nodeFill = "#FADBD8" // soft red
		labelSuffix = " 📄"
	default:
		nodeFill = "#F0F3F4"
	}
	// Apply gradient simulation and enhanced border styling.
	sb.WriteString(fmt.Sprintf("%s\"%s\" [label=\"%s%s\", fontcolor=\"#2C3E50\", fillcolor=\"%s\", style=\"filled,rounded\", penwidth=2, gradientangle=135];\n",
		indent, prefixedID, node.Label, labelSuffix, nodeFill))
}

// Refined renderEdges with modern aesthetics.
func renderEdges(sb *strings.Builder, node *Node, indent string, prefix ...string) {
	prefixedID := fmt.Sprintf("%s%s", strings.Join(prefix, ""), node.ID)
	for _, edge := range node.Edges {
		edgeStyle := "solid"
		edgeColor := "#7F8C8D"
		labelSuffix := ""
		switch edge.Type {
		case Iterator:
			edgeStyle = "dashed"
			edgeColor = "#5DADE2"
			labelSuffix = " [Iter]"
		case Simple:
			edgeStyle = "solid"
			edgeColor = "#7F8C8D"
		}
		toPrefixedID := fmt.Sprintf("%s%s", strings.Join(prefix, ""), edge.To.ID)
		sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\" [label=\"%s%s\", color=\"%s\", style=\"%s\", penwidth=1];\n",
			indent, prefixedID, toPrefixedID, edge.Label, labelSuffix, edgeColor, edgeStyle))
	}
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
		fmt.Println("Not found", v)
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
