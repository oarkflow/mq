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
	inRecursionStack := make(map[string]bool) // track nodes in the recursion stack for cycle detection
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
	inRecursionStack[v] = true // mark node as part of recursion stack
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
	inRecursionStack[v] = false // remove from recursion stack after finishing processing
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

func (tm *DAG) SaveDOTFile(filename string) error {
	dotContent := tm.ExportDOT()
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

func (tm *DAG) ExportDOT() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`digraph "%s" {`, tm.name))
	sb.WriteString("\n")
	sb.WriteString(`  label="Enhanced DAG Representation";`)
	sb.WriteString("\n")
	sb.WriteString(`  labelloc="t"; fontsize=22; fontname="Helvetica";`)
	sb.WriteString("\n")
	sb.WriteString(`  node [shape=box, fontname="Helvetica", fillcolor="#B3CDE0", fontcolor="#2C3E50", fontsize=10, margin="0.25,0.15", style="rounded,filled"];`)
	sb.WriteString("\n")
	sb.WriteString(`  edge [fontname="Helvetica", fontsize=12, arrowsize=0.8];`)
	sb.WriteString("\n")
	sb.WriteString(`  rankdir=TB;`)
	sb.WriteString("\n")
	sortedNodes := tm.TopologicalSort()
	for _, nodeKey := range sortedNodes {
		node, _ := tm.nodes.Get(nodeKey)
		nodeColor := "lightgray"
		nodeShape := "box"
		labelSuffix := ""

		// Apply styles based on NodeType
		switch node.NodeType {
		case Function:
			nodeColor = "#D4EDDA"
			labelSuffix = " [Function]"
		case Page:
			nodeColor = "#f0d2d1"
			labelSuffix = " [Page]"
		}
		sb.WriteString(fmt.Sprintf(
			`  "%s" [label="%s%s",  fontcolor="#2C3E50", fillcolor="%s", shape="%s", style="rounded,filled", id="node_%s"];`,
			node.ID, node.Label, labelSuffix, nodeColor, nodeShape, node.ID))
		sb.WriteString("\n")
	}

	// Define edges with unique styling by EdgeType
	for _, nodeKey := range sortedNodes {
		node, _ := tm.nodes.Get(nodeKey)
		for _, edge := range node.Edges {
			edgeStyle := "solid"
			edgeColor := "black"
			labelSuffix := ""

			// Apply styles based on EdgeType
			switch edge.Type {
			case Iterator:
				edgeStyle = "dashed"
				edgeColor = "blue"
				labelSuffix = " [Iter]"
			case Simple:
				edgeStyle = "solid"
				edgeColor = "black"
				labelSuffix = ""
			}
			sb.WriteString(fmt.Sprintf(
				`  "%s" -> "%s" [label="%s%s", color="%s", style="%s"];`,
				node.ID, edge.To.ID, edge.Label, labelSuffix, edgeColor, edgeStyle))
			sb.WriteString("\n")
		}
	}
	for fromNodeKey, conditions := range tm.conditions {
		for when, then := range conditions {
			if toNode, ok := tm.nodes.Get(then); ok {
				sb.WriteString(fmt.Sprintf(`  "%s" -> "%s" [label=" %s", color="purple", style=dotted, fontsize=10, arrowsize=0.6];`, fromNodeKey, toNode.ID, when))
				sb.WriteString("\n")
			}
		}
	}

	// Optional: Group related nodes into subgraphs (e.g., loops)
	for _, nodeKey := range sortedNodes {
		node, _ := tm.nodes.Get(nodeKey)
		if node.processor != nil {
			subDAG, _ := isDAGNode(node)
			if subDAG != nil {
				sb.WriteString(fmt.Sprintf(`  subgraph "cluster_%s" {`, subDAG.name))
				sb.WriteString("\n")
				sb.WriteString(fmt.Sprintf(`    label="Subgraph: %s";`, subDAG.name))
				sb.WriteString("\n")
				sb.WriteString(`    style=filled; color=gray90;`)
				sb.WriteString("\n")
				subDAG.nodes.ForEach(func(subNodeKey string, subNode *Node) bool {
					sb.WriteString(fmt.Sprintf(`    "%s" [label="%s"];`, subNode.ID, subNode.Label))
					sb.WriteString("\n")
					return true
				})
				subDAG.nodes.ForEach(func(subNodeKey string, subNode *Node) bool {
					for _, edge := range subNode.Edges {
						sb.WriteString(fmt.Sprintf(`    "%s" -> "%s" [label="%s"];`, subNodeKey, edge.To.ID, edge.Label))
						sb.WriteString("\n")
					}
					return true
				})
				sb.WriteString("  }\n")
			}
		}
	}

	sb.WriteString("}\n")
	return sb.String()
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
