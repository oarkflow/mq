package v1

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func (tm *DAG) PrintGraph() {
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
		var edges []string
		for _, edge := range node.Edges {
			for _, target := range edge.To {
				edges = append(edges, fmt.Sprintf("%s (%s)", target.Name, target.Key))
			}
		}
		fmt.Println(strings.Join(edges, ", "))
	}
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
			startNode = firstNode.Key
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
	node := tm.nodes[v]
	hasCycle := false
	var err error
	for _, edge := range node.Edges {
		for _, adj := range edge.To {
			if !visited[adj.Key] {
				builder.WriteString(fmt.Sprintf("Traversing Edge: %s -> %s\n", v, adj.Key))
				hasCycle, err := tm.dfs(adj.Key, visited, discoveryTime, finishedTime, timeVal, inRecursionStack, builder)
				if err != nil {
					return true, err
				}
				if hasCycle {
					return true, nil
				}
			} else if inRecursionStack[adj.Key] {
				cycleMsg := fmt.Sprintf("Cycle detected: %s -> %s\n", v, adj.Key)
				return true, fmt.Errorf(cycleMsg)
			}
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
	node := tm.nodes[v]
	for when, then := range tm.conditions[FromNode(node.Key)] {
		if targetNode, ok := tm.nodes[string(then)]; ok {
			if !visited[targetNode.Key] {
				builder.WriteString(fmt.Sprintf("Traversing Conditional Edge [%s]: %s -> %s\n", when, v, targetNode.Key))
				hasCycle, err := tm.dfs(targetNode.Key, visited, discoveryTime, finishedTime, time, inRecursionStack, builder)
				if err != nil {
					return true, err
				}
				if hasCycle {
					return true, nil
				}
			} else if inRecursionStack[targetNode.Key] {
				cycleMsg := fmt.Sprintf("Cycle detected in Conditional Edge [%s]: %s -> %s\n", when, v, targetNode.Key)
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
	sb.WriteString(fmt.Sprintf(`  label="%s";`, tm.name))
	sb.WriteString("\n")
	sb.WriteString(`  labelloc="t";`)
	sb.WriteString("\n")
	sb.WriteString(`  fontsize=20;`)
	sb.WriteString("\n")
	sb.WriteString(`  node [shape=box, style="rounded,filled", fillcolor="lightgray", fontname="Arial", margin="0.2,0.1"];`)
	sb.WriteString("\n")
	sb.WriteString(`  edge [fontname="Arial", fontsize=12, arrowsize=0.8];`)
	sb.WriteString("\n")
	sb.WriteString(`  size="10,10";`)
	sb.WriteString("\n")
	sb.WriteString(`  ratio="fill";`)
	sb.WriteString("\n")
	sortedNodes := tm.TopologicalSort()
	for _, nodeKey := range sortedNodes {
		node := tm.nodes[nodeKey]
		nodeColor := "lightblue"
		sb.WriteString(fmt.Sprintf(`  "%s" [label=" %s", fillcolor="%s", id="node_%s"];`, node.Key, node.Name, nodeColor, node.Key))
		sb.WriteString("\n")
	}
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
				sb.WriteString(fmt.Sprintf(`  "%s" -> "%s" [label=" %s", color="%s", style=%s, fontsize=10, arrowsize=0.6];`, node.Key, to.Key, edge.Label, edgeColor, edgeStyle))
				sb.WriteString("\n")
			}
		}
	}
	for fromNodeKey, conditions := range tm.conditions {
		for when, then := range conditions {
			if toNode, ok := tm.nodes[string(then)]; ok {
				sb.WriteString(fmt.Sprintf(`  "%s" -> "%s" [label=" %s", color="purple", style=dotted, fontsize=10, arrowsize=0.6];`, fromNodeKey, toNode.Key, when))
				sb.WriteString("\n")
			}
		}
	}
	for _, nodeKey := range sortedNodes {
		node := tm.nodes[nodeKey]
		if node.processor != nil {
			subDAG, _ := isDAGNode(node)
			if subDAG != nil {
				sb.WriteString(fmt.Sprintf(`  subgraph "cluster_%s" {`, subDAG.name))
				sb.WriteString("\n")
				sb.WriteString(fmt.Sprintf(`    label=" %s";`, subDAG.name))
				sb.WriteString("\n")
				sb.WriteString(`    style=dashed;`)
				sb.WriteString("\n")
				sb.WriteString(`    bgcolor="lightgray";`)
				sb.WriteString("\n")
				sb.WriteString(`    node [shape=rectangle, style="filled", fillcolor="lightblue", fontname="Arial", margin="0.2,0.1"];`)
				sb.WriteString("\n")
				for subNodeKey, subNode := range subDAG.nodes {
					sb.WriteString(fmt.Sprintf(`    "%s" [label=" %s"];`, subNodeKey, subNode.Name))
					sb.WriteString("\n")
				}
				for subNodeKey, subNode := range subDAG.nodes {
					for _, edge := range subNode.Edges {
						for _, to := range edge.To {
							sb.WriteString(fmt.Sprintf(`    "%s" -> "%s" [label=" %s", color="black", style=solid, arrowsize=0.6];`, subNodeKey, to.Key, edge.Label))
							sb.WriteString("\n")
						}
					}
				}
				sb.WriteString(`  }`)
				sb.WriteString("\n")
				sb.WriteString(fmt.Sprintf(`  "%s" -> "%s" [label=" %s", color="black", style=solid, arrowsize=0.6];`, node.Key, subDAG.startNode, subDAG.name))
				sb.WriteString("\n")
			}
		}
	}
	sb.WriteString(`}`)
	sb.WriteString("\n")
	return sb.String()
}

func (tm *DAG) TopologicalSort() (stack []string) {
	visited := make(map[string]bool)
	for _, node := range tm.nodes {
		if !visited[node.Key] {
			tm.topologicalSortUtil(node.Key, visited, &stack)
		}
	}
	for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
		stack[i], stack[j] = stack[j], stack[i]
	}
	return
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
