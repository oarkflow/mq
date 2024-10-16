package dag

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func (tm *DAG) PrintGraph() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	fmt.Println("DAG Graph structure:")
	for _, node := range tm.nodes {
		fmt.Printf("Node: %s (%s) -> ", node.Name, node.Key)
		if conditions, ok := tm.conditions[FromNode(node.Key)]; ok {
			var c []string
			for when, then := range conditions {
				if target, ok := tm.nodes[string(then)]; ok {
					c = append(c, fmt.Sprintf("If [%s] Then %s (%s)", when, target.Name, target.Key))
				}
			}
			fmt.Println(strings.Join(c, ", "))
		}
		var c []string
		for _, edge := range node.Edges {
			for _, target := range edge.To {
				c = append(c, fmt.Sprintf("%s (%s)", target.Name, target.Key))
			}
		}
		fmt.Println(strings.Join(c, ", "))
	}
}

func (tm *DAG) ClassifyEdges(startNodes ...string) {
	startNode := tm.GetStartNode()
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if len(startNodes) > 0 && startNodes[0] != "" {
		startNode = startNodes[0]
	}
	visited := make(map[string]bool)
	discoveryTime := make(map[string]int)
	finishedTime := make(map[string]int)
	timeVal := 0
	if startNode == "" {
		firstNode := tm.findStartNode()
		if firstNode != nil {
			startNode = firstNode.Key
		}
	}
	if startNode != "" {
		tm.dfs(startNode, visited, discoveryTime, finishedTime, &timeVal)
	}
}

func (tm *DAG) dfs(v string, visited map[string]bool, discoveryTime, finishedTime map[string]int, timeVal *int) {
	visited[v] = true
	*timeVal++
	discoveryTime[v] = *timeVal
	node := tm.nodes[v]
	for _, edge := range node.Edges {
		for _, adj := range edge.To {
			switch edge.Type {
			case Simple:
				if !visited[adj.Key] {
					fmt.Printf("Simple Edge: %s -> %s\n", v, adj.Key)
					tm.dfs(adj.Key, visited, discoveryTime, finishedTime, timeVal)
				}
			case Iterator:
				if !visited[adj.Key] {
					fmt.Printf("Iterator Edge: %s -> %s\n", v, adj.Key)
					tm.dfs(adj.Key, visited, discoveryTime, finishedTime, timeVal)
				}
			}

		}
	}
	tm.handleConditionalEdges(v, visited, discoveryTime, finishedTime, timeVal)
	*timeVal++
	finishedTime[v] = *timeVal
}

func (tm *DAG) handleConditionalEdges(v string, visited map[string]bool, discoveryTime, finishedTime map[string]int, time *int) {
	node := tm.nodes[v]
	for when, then := range tm.conditions[FromNode(node.Key)] {
		if targetNodeKey, ok := tm.nodes[string(then)]; ok {
			if !visited[targetNodeKey.Key] {
				fmt.Printf("Conditional Edge [%s]: %s -> %s\n", when, v, targetNodeKey.Key)
				tm.dfs(targetNodeKey.Key, visited, discoveryTime, finishedTime, time)
			} else {
				if discoveryTime[v] > discoveryTime[targetNodeKey.Key] {
					fmt.Printf("Conditional Loop Edge [%s]: %s -> %s\n", when, v, targetNodeKey.Key)
				}
			}
		}
	}
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
		os.Remove(dotFile)
	}()
	cmd := exec.Command("dot", arg, dotFile, "-o", fileName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to convert image: %w", err)
	}
	return nil
}

func (tm *DAG) ExportDOT() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("digraph \"%s\" {\n", tm.name))
	sb.WriteString("  node [shape=box, style=\"rounded,filled\", fillcolor=lightgray, fontname=\"Helvetica\"];\n")
	sortedNodes := tm.TopologicalSort()

	// Export nodes
	for _, nodeKey := range sortedNodes {
		node := tm.nodes[nodeKey]
		nodeColor := "lightblue"
		sb.WriteString(fmt.Sprintf("  \"%s\" [label=\"%s\", fillcolor=\"%s\"];\n", node.Key, node.Name, nodeColor))

		// If the node has a sub-DAG, export it
		if subDAG, ok := node.processor.(*DAG); ok && subDAG != nil {
			subDAGName := fmt.Sprintf("%s_sub", node.Key)
			sb.WriteString(fmt.Sprintf("  subgraph \"%s\" {\n", subDAGName))
			sb.WriteString(fmt.Sprintf("    label=\"%s\"\n", node.Name))
			sb.WriteString(subDAG.ExportDOT()) // Export the sub-DAG
			sb.WriteString("  }\n")
		}
	}

	// Export edges
	for _, nodeKey := range sortedNodes {
		node := tm.nodes[nodeKey]
		for _, edge := range node.Edges {
			var edgeStyle string
			switch edge.Type {
			case Iterator:
				edgeStyle = "dashed"
			default:
				edgeStyle = "solid"
			}
			edgeColor := "black"
			for _, to := range edge.To {
				sb.WriteString(fmt.Sprintf("  \"%s\" -> \"%s\" [label=\"%s\", color=\"%s\", style=%s];\n", node.Key, to.Key, edge.Label, edgeColor, edgeStyle))
			}
		}
	}

	// Handle conditional edges
	for fromNodeKey, conditions := range tm.conditions {
		for when, then := range conditions {
			if toNode, ok := tm.nodes[string(then)]; ok {
				sb.WriteString(fmt.Sprintf("  \"%s\" -> \"%s\" [label=\"%s\", color=\"purple\", style=dotted];\n", fromNodeKey, toNode.Key, when))
			}
		}
	}
	sb.WriteString("}\n")
	return sb.String()
}

func (tm *DAG) TopologicalSort() []string {
	visited := make(map[string]bool)
	stack := []string{}
	for _, node := range tm.nodes {
		if !visited[node.Key] {
			tm.topologicalSortUtil(node.Key, visited, &stack)
		}
	}

	for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
		stack[i], stack[j] = stack[j], stack[i]
	}

	return stack
}

func (tm *DAG) topologicalSortUtil(v string, visited map[string]bool, stack *[]string) {
	visited[v] = true
	node := tm.nodes[v]

	for _, edge := range node.Edges {
		for _, to := range edge.To {
			if !visited[to.Key] {
				tm.topologicalSortUtil(to.Key, visited, stack)
			}
		}
	}

	*stack = append(*stack, v)
}
