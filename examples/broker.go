package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type DataItem map[string]interface{}

type NodeInfo struct {
	Name string
	Conn net.Conn
}

type Broker struct {
	nodes      map[string]NodeInfo
	edges      map[string]string
	loops      map[string][]string
	conditions map[string]ConditionConfig
	results    map[string][]DataItem // Track task results by task ID
	mu         sync.Mutex
}

type ConditionConfig struct {
	TrueNode  string
	FalseNode string
}

func NewBroker() *Broker {
	return &Broker{
		nodes:      make(map[string]NodeInfo),
		edges:      make(map[string]string),
		loops:      make(map[string][]string),
		conditions: make(map[string]ConditionConfig),
		results:    make(map[string][]DataItem),
	}
}

func (b *Broker) RegisterNode(name string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fmt.Printf("Registering node: %s\n", name)
	b.nodes[name] = NodeInfo{Name: name, Conn: conn}
}

func (b *Broker) AddEdge(fromNode string, toNode string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fmt.Printf("Adding edge from %s to %s\n", fromNode, toNode)
	b.edges[fromNode] = toNode
}

func (b *Broker) AddLoop(loopNode string, targetNodes []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fmt.Printf("Adding loop at %s with targets: %v\n", loopNode, targetNodes)
	b.loops[loopNode] = targetNodes
}

func (b *Broker) AddCondition(condNode string, trueNode string, falseNode string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fmt.Printf("Adding condition at %s, True: %s, False: %s\n", condNode, trueNode, falseNode)
	b.conditions[condNode] = ConditionConfig{
		TrueNode:  trueNode,
		FalseNode: falseNode,
	}
}

func (b *Broker) SendDataToNode(nodeName string, taskID string, data []DataItem, resultChannel chan []DataItem) {
	b.mu.Lock()
	node, exists := b.nodes[nodeName]
	b.mu.Unlock()
	if !exists {
		fmt.Printf("Node %s not found!\n", nodeName)
		return
	}

	fmt.Printf("Sending data to %s for task %s...\n", nodeName, taskID)
	encoder := json.NewEncoder(node.Conn)
	err := encoder.Encode(data)
	if err != nil {
		fmt.Printf("Error sending data to %s: %v\n", nodeName, err)
		return
	}

	// Receive the processed data back from the node asynchronously
	go func() {
		decoder := json.NewDecoder(node.Conn)
		var result []DataItem
		err = decoder.Decode(&result)
		if err != nil {
			fmt.Printf("Error receiving data from %s for task %s: %v\n", nodeName, taskID, err)
			return
		}
		fmt.Printf("Received processed data from %s for task %s\n", nodeName, taskID)

		// Send the result to the result aggregation channel
		resultChannel <- result
	}()
}

func (b *Broker) DispatchData(startNode string, data []DataItem, taskID string) []DataItem {
	finalResult := []DataItem{}
	currentNode := startNode
	resultChannel := make(chan []DataItem, len(data)) // Create a channel to handle async results

	for {
		b.mu.Lock()
		nextNode, hasEdge := b.edges[currentNode]
		loopTargets, hasLoop := b.loops[currentNode]
		conditionConfig, hasCondition := b.conditions[currentNode]
		b.mu.Unlock()

		// Handle Loops (async dispatch)
		if hasLoop {
			var wg sync.WaitGroup
			fmt.Printf("Dispatching to loop nodes from %s for task %s...\n", currentNode, taskID)
			for _, targetNode := range loopTargets {
				wg.Add(1)
				go func(node string) {
					defer wg.Done()
					b.SendDataToNode(node, taskID, data, resultChannel)
				}(targetNode)
			}

			// Wait for loop processing to complete
			go func() {
				wg.Wait()
				close(resultChannel)
			}()

			// Collect async results
			for res := range resultChannel {
				finalResult = append(finalResult, res...)
			}

			b.AggregateResults(taskID, finalResult)
			return finalResult // Exit after loop processing
		}

		// Handle Conditions
		if hasCondition {
			for _, item := range data {
				resultChannel := make(chan []DataItem, 1)
				go b.SendDataToNode(currentNode, taskID, []DataItem{item}, resultChannel)

				select {
				case result := <-resultChannel:
					nextNode = conditionConfig.TrueNode
					finalResult = append(finalResult, b.DispatchData(nextNode, result, taskID)...)
				case <-time.After(5 * time.Second): // Timeout if no response
					fmt.Printf("Condition check timed out at node: %s\n", currentNode)
					nextNode = conditionConfig.FalseNode
				}
			}
			b.AggregateResults(taskID, finalResult)
			return finalResult // Exit after condition processing
		}

		// Handle simple edges (sequential flow)
		if hasEdge {
			b.SendDataToNode(currentNode, taskID, data, resultChannel)

			select {
			case result := <-resultChannel:
				currentNode = nextNode
				data = result
			case <-time.After(5 * time.Second): // Timeout if no response
				fmt.Printf("Processing timed out at node: %s\n", currentNode)
				return finalResult
			}
		} else {
			fmt.Printf("No edge found for node: %s, stopping...\n", currentNode)
			break
		}
	}

	b.AggregateResults(taskID, finalResult)
	return finalResult
}

func (b *Broker) AggregateResults(taskID string, result []DataItem) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.results[taskID] = append(b.results[taskID], result...)
	fmt.Printf("Aggregated result for task %s: %v\n", taskID, b.results[taskID])
}

func (b *Broker) HandleConnections() {
	listener, err := net.Listen("tcp", ":8081")
	if err != nil {
		fmt.Println("Error setting up TCP server:", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Broker is listening on port 8081...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go func(conn net.Conn) {
			defer conn.Close()
			reader := bufio.NewReader(conn)
			nodeName, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading node name:", err)
				return
			}
			nodeName = strings.TrimSpace(nodeName)

			b.RegisterNode(nodeName, conn)
		}(conn)
	}
}

func main() {
	broker := NewBroker()

	// Set up the flow
	broker.AddEdge("Node1", "Node2")
	broker.AddLoop("Node2", []string{"Node3"})
	broker.AddCondition("Node3", "Node4", "")

	// Start the broker to listen for node connections
	go broker.HandleConnections()

	fmt.Println("Press ENTER to start the flow after nodes are connected...")
	bufio.NewReader(os.Stdin).ReadString('\n')

	// Example Data Items
	dataItems := []DataItem{
		{"id": 1, "value": "item1"},
		{"id": 2, "value": "item2"},
		{"id": 3, "value": "item3"},
	}

	taskID := "task-001" // Unique ID to track this task
	finalResult := broker.DispatchData("Node1", dataItems, taskID)
	fmt.Println("Final result after processing:", finalResult)
}
