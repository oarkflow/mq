package dag

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
)

// EdgeLevel represents the depth level of an edge in the DAG
type EdgeLevel struct {
	Level int
	Type  EdgeType
}

// EdgeStyleConfig contains styling information for edges
type EdgeStyleConfig struct {
	Color     string
	Style     string
	PenWidth  string
	ArrowSize string
	FontSize  string
}

// calculateEdgeLevels dynamically calculates the level of each edge based on DAG structure
func (tm *DAG) calculateEdgeLevels() map[string]EdgeLevel {
	levels := make(map[string]int)
	edgeLevels := make(map[string]EdgeLevel)

	// Find start nodes (nodes with no incoming edges)
	inDegree := make(map[string]int)
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		inDegree[node.ID] = 0
		return true
	})

	// Calculate in-degrees
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		for _, edge := range node.Edges {
			inDegree[edge.To.ID]++
		}
		return true
	})

	// Also count conditional edges
	for _, conditions := range tm.conditions {
		for _, toNodeID := range conditions {
			inDegree[toNodeID]++
		}
	}

	// BFS to calculate levels
	queue := make([]string, 0)
	for nodeID, degree := range inDegree {
		if degree == 0 {
			levels[nodeID] = 0
			queue = append(queue, nodeID)
		}
	}

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]
		currentLevel := levels[currentID]

		if node, exists := tm.nodes.Get(currentID); exists {
			// Process regular edges
			for _, edge := range node.Edges {
				newLevel := currentLevel + 1
				if existingLevel, hasLevel := levels[edge.To.ID]; !hasLevel || newLevel > existingLevel {
					levels[edge.To.ID] = newLevel
					queue = append(queue, edge.To.ID)
				}

				// Store edge level information
				edgeKey := fmt.Sprintf("%s->%s", currentID, edge.To.ID)
				edgeLevels[edgeKey] = EdgeLevel{
					Level: currentLevel,
					Type:  edge.Type,
				}
			}
		}

		// Process conditional edges
		if conditions, exists := tm.conditions[currentID]; exists {
			for _, toNodeID := range conditions {
				newLevel := currentLevel + 1
				if existingLevel, hasLevel := levels[toNodeID]; !hasLevel || newLevel > existingLevel {
					levels[toNodeID] = newLevel
					queue = append(queue, toNodeID)
				}

				// Store conditional edge level
				edgeKey := fmt.Sprintf("%s->%s:conditional", currentID, toNodeID)
				edgeLevels[edgeKey] = EdgeLevel{
					Level: currentLevel,
					Type:  Simple, // Conditional edges are treated as Simple type
				}
			}
		}
	}

	return edgeLevels
}

// getEdgeStyle returns the appropriate styling for an edge based on its level and type
func (tm *DAG) getEdgeStyle(level EdgeLevel) EdgeStyleConfig {
	baseColors := []string{
		"#546E7A", // Blue Grey - Level 0 (muted)
		"#689F38", // Dark Green - Level 1 (muted)
		"#F57C00", // Dark Orange - Level 2 (muted)
		"#7B1FA2", // Dark Purple - Level 3 (muted)
		"#C62828", // Dark Red - Level 4 (muted)
		"#00838F", // Dark Cyan - Level 5 (muted)
		"#5D4037", // Dark Brown - Level 6 (muted)
		"#455A64", // Dark Blue Grey - Level 7 (muted)
	}

	colorIndex := level.Level % len(baseColors)
	baseColor := baseColors[colorIndex]

	switch level.Type {
	case Iterator:
		return EdgeStyleConfig{
			Color:     baseColor,
			Style:     "bold",
			PenWidth:  "2.0",
			ArrowSize: "0.8",
			FontSize:  "8",
		}
	case Simple:
		return EdgeStyleConfig{
			Color:     baseColor,
			Style:     "solid",
			PenWidth:  "1.2",
			ArrowSize: "0.6",
			FontSize:  "7",
		}
	default:
		return EdgeStyleConfig{
			Color:     baseColor,
			Style:     "solid",
			PenWidth:  "1.2",
			ArrowSize: "0.6",
			FontSize:  "7",
		}
	}
}

// getConditionalEdgeStyle returns styling for conditional edges
func (tm *DAG) getConditionalEdgeStyle(level int) EdgeStyleConfig {
	baseColors := []string{
		"#8D6E63", // Muted Brown - Level 0
		"#AD1457", // Muted Pink - Level 1
		"#512DA8", // Muted Deep Purple - Level 2
		"#303F9F", // Muted Indigo - Level 3
		"#00695C", // Muted Teal - Level 4
		"#689F38", // Muted Light Green - Level 5
		"#827717", // Muted Olive - Level 6
		"#FF8F00", // Muted Amber - Level 7
	}

	colorIndex := level % len(baseColors)

	return EdgeStyleConfig{
		Color:     baseColors[colorIndex],
		Style:     "dashed",
		PenWidth:  "1.5",
		ArrowSize: "0.7",
		FontSize:  "7",
	}
}

// calculateNodeLevels dynamically calculates the level of each node based on DAG structure
func (tm *DAG) calculateNodeLevels() map[string]int {
	levels := make(map[string]int)

	// Find start nodes (nodes with no incoming edges)
	inDegree := make(map[string]int)
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		inDegree[node.ID] = 0
		return true
	})

	// Calculate in-degrees
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		for _, edge := range node.Edges {
			inDegree[edge.To.ID]++
		}
		return true
	})

	// Also count conditional edges
	for _, conditions := range tm.conditions {
		for _, toNodeID := range conditions {
			inDegree[toNodeID]++
		}
	}

	// BFS to calculate levels
	queue := make([]string, 0)
	for nodeID, degree := range inDegree {
		if degree == 0 {
			levels[nodeID] = 0
			queue = append(queue, nodeID)
		}
	}

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]
		currentLevel := levels[currentID]

		if node, exists := tm.nodes.Get(currentID); exists {
			// Process regular edges
			for _, edge := range node.Edges {
				newLevel := currentLevel + 1
				if existingLevel, hasLevel := levels[edge.To.ID]; !hasLevel || newLevel > existingLevel {
					levels[edge.To.ID] = newLevel
					queue = append(queue, edge.To.ID)
				}
			}
		}

		// Process conditional edges
		if conditions, exists := tm.conditions[currentID]; exists {
			for _, toNodeID := range conditions {
				newLevel := currentLevel + 1
				if existingLevel, hasLevel := levels[toNodeID]; !hasLevel || newLevel > existingLevel {
					levels[toNodeID] = newLevel
					queue = append(queue, toNodeID)
				}
			}
		}
	}

	return levels
}

// getNodeStyle returns professional styling for nodes based on their level and type
func (tm *DAG) getNodeStyle(nodeType NodeType, level int) (fillColor, shape, borderColor string) {
	// Muted color palettes for different levels
	levelColors := []string{
		"#ECEFF1", // Blue Grey 50 - Level 0
		"#F1F8E9", // Light Green 50 - Level 1
		"#FFF3E0", // Orange 50 - Level 2
		"#F3E5F5", // Purple 50 - Level 3
		"#FFEBEE", // Red 50 - Level 4
		"#E0F2F1", // Teal 50 - Level 5
		"#EFEBE9", // Brown 50 - Level 6
		"#FAFAFA", // Grey 50 - Level 7
	}

	levelBorders := []string{
		"#546E7A", // Blue Grey 600 - Level 0
		"#689F38", // Light Green 700 - Level 1
		"#F57C00", // Orange 700 - Level 2
		"#7B1FA2", // Purple 700 - Level 3
		"#C62828", // Red 700 - Level 4
		"#00838F", // Teal 700 - Level 5
		"#5D4037", // Brown 700 - Level 6
		"#455A64", // Blue Grey 700 - Level 7
	}

	colorIndex := level % len(levelColors)
	baseFill := levelColors[colorIndex]
	baseBorder := levelBorders[colorIndex]

	switch nodeType {
	case Function:
		return baseFill, "box", baseBorder
	case Page:
		return baseFill, "box", baseBorder
	default:
		return baseFill, "ellipse", baseBorder
	}
}

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

func (tm *DAG) ExportDOT(direction ...Direction) string {
	rankDir := TB
	if len(direction) > 0 && direction[0] != "" {
		rankDir = direction[0]
	}
	var sb strings.Builder

	// Graph header and global attributes
	sb.WriteString(fmt.Sprintf("digraph \"%s\" {\n", tm.name))
	sb.WriteString("  graph [\n")
	sb.WriteString("    rankdir=" + string(rankDir) + ",\n")
	sb.WriteString("    bgcolor=\"#FAFAFA\",\n")
	sb.WriteString("    pad=\"0.5\",\n")
	sb.WriteString("    nodesep=\"0.4\",\n")
	sb.WriteString("    ranksep=\"0.7\",\n")
	sb.WriteString("    splines=polyline,\n")
	sb.WriteString("    overlap=scale,\n")
	sb.WriteString("    concentrate=true,\n")
	sb.WriteString("    layout=dot,\n")
	sb.WriteString("    center=true\n")
	sb.WriteString("  ];\n")

	// Add professional color legend as a comment
	sb.WriteString("  // Professional DAG Visualization\n")
	sb.WriteString("  // - Muted color scheme for better readability\n")
	sb.WriteString("  // - Level-based node and edge coloring\n")
	sb.WriteString("  // - Simple edges: solid lines (light weight)\n")
	sb.WriteString("  // - Iterator edges: bold lines (medium weight)\n")
	sb.WriteString("  // - Conditional edges: dashed lines (distinct styling)\n\n")

	// Node styling
	sb.WriteString("  node [\n")
	sb.WriteString("    fontname=\"Inter, -apple-system, BlinkMacSystemFont, sans-serif\",\n")
	sb.WriteString("    fontsize=9,\n")
	sb.WriteString("    style=\"filled,rounded\",\n")
	sb.WriteString("    penwidth=1.5,\n")
	sb.WriteString("    margin=\"0.15,0.08\",\n")
	sb.WriteString("    width=0, height=0, fixedsize=false\n")
	sb.WriteString("  ];\n")

	// Edge styling
	sb.WriteString("  edge [\n")
	sb.WriteString("    fontname=\"Inter, -apple-system, BlinkMacSystemFont, sans-serif\",\n")
	sb.WriteString("    fontsize=8,\n")
	sb.WriteString("    color=\"#666666\",\n")
	sb.WriteString("    penwidth=1.0,\n")
	sb.WriteString("    arrowsize=0.6,\n")
	sb.WriteString("    minlen=1,\n")
	sb.WriteString("    labelfloat=true,\n")
	sb.WriteString("    labeldistance=5.5,\n")
	sb.WriteString("    labelangle=30\n")
	sb.WriteString("  ];\n\n")

	// Render graph content
	tm.renderCompactDAG(&sb, "  ")
	sb.WriteString("}\n")
	return sb.String()
}

// renderCompactDAG is responsible for outputting nodes, clusters, and edges
func (tm *DAG) renderCompactDAG(sb *strings.Builder, indent string) {
	sorted := tm.TopologicalSort()

	// Calculate edge and node levels for dynamic styling
	edgeLevels := tm.calculateEdgeLevels()
	nodeLevels := tm.calculateNodeLevels()

	// Nodes with level-based styling
	for _, id := range sorted {
		node, ok := tm.nodes.Get(id)
		if !ok || tm.isSubDAGNode(node) {
			continue
		}
		nodeLevel := nodeLevels[id]
		tm.renderCompactNode(sb, node, nodeLevel, indent)
	}

	// Sub-DAG clusters
	hasSub := false
	for _, id := range sorted {
		node, _ := tm.nodes.Get(id)
		if sub, ok := isDAGNode(node); ok && sub.consumerTopic != "" {
			if !hasSub {
				sb.WriteString(indent + "// Sub-workflows\n")
				hasSub = true
			}
			tm.renderCompactSubDAGCluster(sb, id, sub, indent)
		}
	}

	// Edges with level-based styling
	for _, id := range sorted {
		node, _ := tm.nodes.Get(id)
		tm.renderCompactEdges(sb, node, edgeLevels, indent)
	}

	// Conditional edges with level-based styling
	if len(tm.conditions) > 0 {
		sb.WriteString("\n")
		tm.renderCompactConditionalEdges(sb, edgeLevels, indent)
	}
}

// renderCompactNode renders a single node with professional level-based styling
func (tm *DAG) renderCompactNode(sb *strings.Builder, node *Node, nodeLevel int, indent string) {
	var fillColor, shape, borderColor string

	// Check if this is a sub-DAG node
	if subDAG, ok := isDAGNode(node); ok && subDAG.consumerTopic != "" {
		fillColor = "#E3F2FD"
		shape = "box"
		borderColor = "#1976D2"
	} else {
		fillColor, shape, borderColor = tm.getNodeStyle(node.NodeType, nodeLevel)
	}

	// Clean, simple label
	cleanLabel := strings.ReplaceAll(node.Label, `"`, `\"`)

	sb.WriteString(fmt.Sprintf("%s\"%s\" [", indent, node.ID))
	sb.WriteString(fmt.Sprintf(`label=" \n %s \n ", `, cleanLabel))
	sb.WriteString(fmt.Sprintf(`fillcolor="%s", `, fillColor))
	sb.WriteString(fmt.Sprintf(`color="%s", `, borderColor))
	sb.WriteString(fmt.Sprintf(`shape=%s`, shape))
	sb.WriteString("];\n")
}

// renderCompactSubDAGCluster renders sub-DAG with clean cluster styling
func (tm *DAG) renderCompactSubDAGCluster(sb *strings.Builder, parentNodeID string, subDAG *DAG, indent string) {
	clusterName := fmt.Sprintf("cluster_%s", parentNodeID)

	// Calculate edge levels and node levels for the sub-DAG
	subEdgeLevels := subDAG.calculateEdgeLevels()
	subNodeLevels := subDAG.calculateNodeLevels()

	sb.WriteString(fmt.Sprintf("%ssubgraph \"%s\" {\n", indent, clusterName))
	sb.WriteString(fmt.Sprintf("%s  label=\"%s\";\n", indent, subDAG.name))
	sb.WriteString(fmt.Sprintf("%s  style=\"dashed\";\n", indent))
	sb.WriteString(fmt.Sprintf("%s  color=\"#BDBDBD\";\n", indent))
	sb.WriteString(fmt.Sprintf("%s  fontsize=8;\n", indent))
	sb.WriteString(fmt.Sprintf("%s  bgcolor=\"#FAFAFA\";\n", indent))

	// Render sub-DAG nodes with level-based styling
	subSortedNodes := subDAG.TopologicalSort()
	for _, subNodeID := range subSortedNodes {
		subNode, exists := subDAG.nodes.Get(subNodeID)
		if !exists {
			continue
		}
		prefixedID := fmt.Sprintf("%s_%s", parentNodeID, subNodeID)
		nodeLevel := subNodeLevels[subNodeID]
		tm.renderCompactPrefixedNode(sb, subNode, prefixedID, indent+"  ", nodeLevel)
	}

	// Render sub-DAG edges with level-based styling
	for _, subNodeID := range subSortedNodes {
		subNode, exists := subDAG.nodes.Get(subNodeID)
		if !exists {
			continue
		}
		tm.renderCompactPrefixedEdges(sb, subNode, parentNodeID, indent+"  ", subEdgeLevels)
	}

	// Render sub-DAG conditional edges with level-based styling
	for fromNodeID, conditions := range subDAG.conditions {
		for condition, toNodeID := range conditions {
			fromPrefixed := fmt.Sprintf("%s_%s", parentNodeID, fromNodeID)
			toPrefixed := fmt.Sprintf("%s_%s", parentNodeID, toNodeID)

			// Calculate level for conditional edge
			edgeKey := fmt.Sprintf("%s->%s:conditional", fromNodeID, toNodeID)
			var level int
			if edgeLevel, exists := subEdgeLevels[edgeKey]; exists {
				level = edgeLevel.Level
			} else {
				level = 0
			}

			style := subDAG.getConditionalEdgeStyle(level)
			cleanCondition := strings.ReplaceAll(condition, `"`, `\"`)

			sb.WriteString(fmt.Sprintf("%s  \"%s\" -> \"%s\" [", indent, fromPrefixed, toPrefixed))
			sb.WriteString(fmt.Sprintf(`color="%s", `, style.Color))
			sb.WriteString(fmt.Sprintf(`style="%s", `, style.Style))
			sb.WriteString(fmt.Sprintf(`penwidth=%s, `, style.PenWidth))
			sb.WriteString(fmt.Sprintf(`arrowsize=%s, `, style.ArrowSize))
			sb.WriteString(fmt.Sprintf(`fontsize=%s, `, style.FontSize))
			sb.WriteString(fmt.Sprintf(`label="%s", `, cleanCondition))
			sb.WriteString(fmt.Sprintf(`tooltip="Level %d - Conditional", `, level))
			sb.WriteString(fmt.Sprintf(`labeldistance=%.1f, `, math.Max(1.1, math.Min(1.3, float64(level)*0.1+1.1))))
			sb.WriteString(`labelangle=0`)
			sb.WriteString("];\n")
		}
	}

	sb.WriteString(fmt.Sprintf("%s}\n", indent))
}

// renderCompactPrefixedNode renders a prefixed node with professional level-based styling
func (tm *DAG) renderCompactPrefixedNode(sb *strings.Builder, node *Node, prefixedID, indent string, nodeLevel int) {
	fillColor, shape, borderColor := tm.getNodeStyle(node.NodeType, nodeLevel)

	cleanLabel := strings.ReplaceAll(node.Label, `"`, `\"`)

	sb.WriteString(fmt.Sprintf("%s\"%s\" [", indent, prefixedID))
	sb.WriteString(fmt.Sprintf(`label="%s", `, cleanLabel))
	sb.WriteString(fmt.Sprintf(`fillcolor="%s", `, fillColor))
	sb.WriteString(fmt.Sprintf(`color="%s", `, borderColor))
	sb.WriteString(fmt.Sprintf(`shape=%s, `, shape))
	sb.WriteString(`fontsize=8`)
	sb.WriteString("];\n")
}

// renderCompactEdges dynamically adjusts label positions and dimensions
func (tm *DAG) renderCompactEdges(sb *strings.Builder, node *Node, edgeLevels map[string]EdgeLevel, indent string) {
	for _, edge := range node.Edges {
		fromID := strings.Join(strings.Split(edge.FromSource, "."), "_")
		edgeKey := fmt.Sprintf("%s->%s", node.ID, edge.To.ID)

		// Get edge level and styling
		edgeLevel, exists := edgeLevels[edgeKey]
		if !exists {
			// Fallback to default styling
			edgeLevel = EdgeLevel{Level: 0, Type: edge.Type}
		}
		style := tm.getEdgeStyle(edgeLevel)

		sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\"", indent, fromID, edge.To.ID))

		// Apply styling attributes
		sb.WriteString(" [")
		sb.WriteString(fmt.Sprintf(`color="%s", `, style.Color))
		sb.WriteString(fmt.Sprintf(`style="%s", `, style.Style))
		sb.WriteString(fmt.Sprintf(`penwidth=%s, `, style.PenWidth))
		sb.WriteString(fmt.Sprintf(`arrowsize=%s, `, style.ArrowSize))
		sb.WriteString(fmt.Sprintf(`fontsize=%s, `, style.FontSize))

		if edge.Label != "" {
			cleanLabel := strings.ReplaceAll(edge.Label, `"`, `\"`)
			sb.WriteString(fmt.Sprintf(`label="%s", `, cleanLabel))
			sb.WriteString(fmt.Sprintf(`labeldistance=%.1f, `, math.Max(1.1, math.Min(2.0, float64(len(edge.Label))*0.05+1.1))))
			sb.WriteString(fmt.Sprintf(`labelangle=%d`, 0))
		}

		// Add edge type indicator in tooltip for debugging
		edgeTypeLabel := fmt.Sprintf("Level %d - %s", edgeLevel.Level, edgeLevel.Type.String())
		sb.WriteString(fmt.Sprintf(`, tooltip="%s"`, edgeTypeLabel))

		sb.WriteString("];\n")
	}
}

// renderCompactPrefixedEdges renders prefixed edges with professional level-based styling
func (tm *DAG) renderCompactPrefixedEdges(sb *strings.Builder, node *Node, prefix, indent string, edgeLevels map[string]EdgeLevel) {
	fromPrefixed := fmt.Sprintf("%s_%s", prefix, node.ID)
	for _, edge := range node.Edges {
		toPrefixed := fmt.Sprintf("%s_%s", prefix, edge.To.ID)
		edgeKey := fmt.Sprintf("%s->%s", node.ID, edge.To.ID)

		// Get edge level and styling
		edgeLevel, exists := edgeLevels[edgeKey]
		if !exists {
			edgeLevel = EdgeLevel{Level: 0, Type: edge.Type}
		}
		style := tm.getEdgeStyle(edgeLevel)

		sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\"", indent, fromPrefixed, toPrefixed))

		// Apply styling attributes
		sb.WriteString(" [")
		sb.WriteString(fmt.Sprintf(`color="%s", `, style.Color))
		sb.WriteString(fmt.Sprintf(`style="%s", `, style.Style))
		sb.WriteString(fmt.Sprintf(`penwidth=%s, `, style.PenWidth))
		sb.WriteString(fmt.Sprintf(`arrowsize=%s, `, style.ArrowSize))
		sb.WriteString(fmt.Sprintf(`fontsize=%s, `, style.FontSize))

		if edge.Label != "" {
			cleanLabel := strings.ReplaceAll(edge.Label, `"`, `\"`)
			sb.WriteString(fmt.Sprintf(`label="%s", `, cleanLabel))
			sb.WriteString(fmt.Sprintf(`labeldistance=%.1f, `, math.Max(1.1, math.Min(1.3, float64(edgeLevel.Level)*0.1+1.1))))
			sb.WriteString(`labelangle=0`)
		}

		// Add edge type indicator
		edgeTypeLabel := fmt.Sprintf("Level %d - %s", edgeLevel.Level, edgeLevel.Type.String())
		sb.WriteString(fmt.Sprintf(`, tooltip="%s"`, edgeTypeLabel))

		sb.WriteString("];\n")
	}
}

// renderCompactConditionalEdges renders conditional edges with professional level-based styling
func (tm *DAG) renderCompactConditionalEdges(sb *strings.Builder, edgeLevels map[string]EdgeLevel, indent string) {
	for fromNodeID, conditions := range tm.conditions {
		for condition, toNodeID := range conditions {
			edgeKey := fmt.Sprintf("%s->%s:conditional", fromNodeID, toNodeID)

			// Get or calculate level for conditional edge
			var level int
			if edgeLevel, exists := edgeLevels[edgeKey]; exists {
				level = edgeLevel.Level
			} else {
				level = 0 // Default level
			}

			style := tm.getConditionalEdgeStyle(level)
			cleanCondition := strings.ReplaceAll(condition, `"`, `\"`)

			sb.WriteString(fmt.Sprintf("%s\"%s\" -> \"%s\" [", indent, fromNodeID, toNodeID))
			sb.WriteString(fmt.Sprintf(`color="%s", `, style.Color))
			sb.WriteString(fmt.Sprintf(`style="%s", `, style.Style))
			sb.WriteString(fmt.Sprintf(`penwidth=%s, `, style.PenWidth))
			sb.WriteString(fmt.Sprintf(`arrowsize=%s, `, style.ArrowSize))
			sb.WriteString(fmt.Sprintf(`fontsize=%s, `, style.FontSize))
			sb.WriteString(fmt.Sprintf(`label="%s", `, cleanCondition))
			sb.WriteString(fmt.Sprintf(`tooltip="Level %d - Conditional", `, level))
			sb.WriteString(fmt.Sprintf(`labeldistance=%.1f, `, math.Max(1.1, math.Min(1.3, float64(level)*0.1+1.1))))
			sb.WriteString(`labelangle=0`)
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
