package main

import (
	"fmt"
	"sort"
)

// DAG represents a Directed Acyclic Graph
type DAG struct {
	vertices int
	adjList  map[int][]int // adjacency list to represent edges
}

// NewDAG creates a new DAG with a given number of vertices
func NewDAG(vertices int) *DAG {
	return &DAG{
		vertices: vertices,
		adjList:  make(map[int][]int),
	}
}

// AddEdge adds a directed edge from u to v
func (d *DAG) AddEdge(u, v int) {
	d.adjList[u] = append(d.adjList[u], v)
}

// PrintGraph prints the graph's adjacency list
func (d *DAG) PrintGraph() {
	for vertex, edges := range d.adjList {
		fmt.Printf("Vertex %d -> %v\n", vertex, edges)
	}
}

// DFS traversal function to classify edges as tree, forward, or cross
func (d *DAG) ClassifyEdges() {
	visited := make([]bool, d.vertices)
	discoveryTime := make([]int, d.vertices)
	finishedTime := make([]int, d.vertices)
	time := 0

	for i := 0; i < d.vertices; i++ {
		if !visited[i] {
			d.dfs(i, visited, discoveryTime, finishedTime, &time)
		}
	}
}

// dfs performs a DFS and classifies the edges
func (d *DAG) dfs(v int, visited []bool, discoveryTime []int, finishedTime []int, time *int) {
	visited[v] = true
	*time++
	discoveryTime[v] = *time

	for _, adj := range d.adjList[v] {
		if !visited[adj] {
			// Tree Edge: adj not visited, and it's being discovered
			fmt.Printf("Tree Edge: %d -> %d\n", v, adj)
			d.dfs(adj, visited, discoveryTime, finishedTime, time)
		} else {
			if discoveryTime[v] < discoveryTime[adj] {
				// Forward Edge: adj is a descendant but already discovered
				fmt.Printf("Forward Edge: %d -> %d\n", v, adj)
			} else if finishedTime[adj] == 0 {
				// Cross Edge: adj is in a different branch (adj was visited, but not fully processed)
				fmt.Printf("Cross Edge: %d -> %d\n", v, adj)
			}
		}
	}

	*time++
	finishedTime[v] = *time
}

// TopologicalSort returns a topologically sorted order of the DAG vertices
func (d *DAG) TopologicalSort() []int {
	visited := make([]bool, d.vertices)
	stack := []int{}

	for i := 0; i < d.vertices; i++ {
		if !visited[i] {
			d.topologicalSortUtil(i, visited, &stack)
		}
	}

	// Reverse the stack to get the topological order
	sort.Slice(stack, func(i, j int) bool { return stack[i] > stack[j] })
	return stack
}

// Helper function for topological sorting using DFS
func (d *DAG) topologicalSortUtil(v int, visited []bool, stack *[]int) {
	visited[v] = true

	for _, adj := range d.adjList[v] {
		if !visited[adj] {
			d.topologicalSortUtil(adj, visited, stack)
		}
	}

	*stack = append(*stack, v)
}

// Main function to demonstrate DAG edge classification
func main() {
	// Create a new DAG
	dag := NewDAG(6)

	// Add edges (vertices start from 0)
	dag.AddEdge(0, 1)
	dag.AddEdge(0, 2)
	dag.AddEdge(1, 3)
	dag.AddEdge(2, 3)
	dag.AddEdge(3, 4)
	dag.AddEdge(4, 5)

	fmt.Println("Graph adjacency list:")
	dag.PrintGraph()

	fmt.Println("\nClassifying edges:")
	dag.ClassifyEdges()

	// Perform topological sorting
	fmt.Println("\nTopologically sorted order:")
	order := dag.TopologicalSort()
	fmt.Println(order)
}
