package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
)

func enhancedSubDAG() *dag.DAG {
	f := dag.NewDAG("Enhanced Sub DAG", "enhanced-sub-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Enhanced Sub DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))
	f.
		AddNode(dag.Function, "Store data", "store:data", &tasks.StoreData{Operation: dag.Operation{Type: dag.Function}}, true).
		AddNode(dag.Function, "Send SMS", "send:sms", &tasks.SendSms{Operation: dag.Operation{Type: dag.Function}}).
		AddNode(dag.Function, "Notification", "notification", &tasks.InAppNotification{Operation: dag.Operation{Type: dag.Function}}).
		AddEdge(dag.Simple, "Store Payload to send sms", "store:data", "send:sms").
		AddEdge(dag.Simple, "Store Payload to notification", "send:sms", "notification")
	return f
}

func main() {
	fmt.Println("🚀 Starting Simple Enhanced DAG Demo...")

	// Create enhanced DAG - simple configuration, just like regular DAG but with enhanced features
	flow := dag.NewDAG("Enhanced Sample DAG", "enhanced-sample-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Enhanced DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	})

	// Configure memory storage (same as original)
	flow.ConfigureMemoryStorage()

	// Enable enhanced features - this is the only difference from regular DAG
	err := flow.EnableEnhancedFeatures(&dag.EnhancedDAGConfig{
		EnableWorkflowEngine:    true,
		MaintainDAGMode:         true,
		EnableStateManagement:   true,
		EnableAdvancedRetry:     true,
		MaxConcurrentExecutions: 10,
		EnableMetrics:           true,
	})
	if err != nil {
		panic(fmt.Errorf("failed to enable enhanced features: %v", err))
	}

	// Add nodes exactly like the original DAG
	flow.AddNode(dag.Function, "GetData", "GetData", &EnhancedGetData{}, true)
	flow.AddNode(dag.Function, "Loop", "Loop", &EnhancedLoop{})
	flow.AddNode(dag.Function, "ValidateAge", "ValidateAge", &EnhancedValidateAge{})
	flow.AddNode(dag.Function, "ValidateGender", "ValidateGender", &EnhancedValidateGender{})
	flow.AddNode(dag.Function, "Final", "Final", &EnhancedFinal{})
	flow.AddDAGNode(dag.Function, "Check", "persistent", enhancedSubDAG())

	// Add edges exactly like the original DAG
	flow.AddEdge(dag.Simple, "GetData", "GetData", "Loop")
	flow.AddEdge(dag.Iterator, "Validate age for each item", "Loop", "ValidateAge")
	flow.AddCondition("ValidateAge", map[string]string{"pass": "ValidateGender", "default": "persistent"})
	flow.AddEdge(dag.Simple, "Mark as Done", "Loop", "Final")

	// Process data exactly like the original DAG
	data := []byte(`[{"age": "15", "gender": "female"}, {"age": "18", "gender": "male"}]`)
	if flow.Error != nil {
		panic(flow.Error)
	}

	fmt.Println("Processing data with enhanced DAG...")
	start := time.Now()
	rs := flow.Process(context.Background(), data)
	duration := time.Since(start)

	if rs.Error != nil {
		panic(rs.Error)
	}
	fmt.Println("Status:", rs.Status, "Topic:", rs.Topic)
	fmt.Println("Result:", string(rs.Payload))
	fmt.Printf("✅ Enhanced DAG completed successfully in %v!\n", duration)
	fmt.Println("Enhanced features like retry management, metrics, and state management were active during processing.")
}

// Enhanced task implementations - same logic as original but with enhanced logging
type EnhancedGetData struct {
	dag.Operation
}

func (p *EnhancedGetData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("📊 Enhanced GetData: Processing task with enhanced features")
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type EnhancedLoop struct {
	dag.Operation
}

func (p *EnhancedLoop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("🔄 Enhanced Loop: Processing with enhanced retry capabilities")
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type EnhancedValidateAge struct {
	dag.Operation
}

func (p *EnhancedValidateAge) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("✅ Enhanced ValidateAge: Processing with enhanced validation")
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("ValidateAge Error: %s", err.Error()), Ctx: ctx}
	}
	var status string
	if data["age"] == "18" {
		status = "pass"
		fmt.Printf("✅ Age validation passed for age: %s\n", data["age"])
	} else {
		status = "default"
		fmt.Printf("❌ Age validation failed for age: %s\n", data["age"])
	}
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx, ConditionStatus: status}
}

type EnhancedValidateGender struct {
	dag.Operation
}

func (p *EnhancedValidateGender) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("🚻 Enhanced ValidateGender: Processing with enhanced gender validation")
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("ValidateGender Error: %s", err.Error()), Ctx: ctx}
	}
	data["female_voter"] = data["gender"] == "female"
	data["enhanced_processed"] = true // Mark as processed by enhanced DAG
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type EnhancedFinal struct {
	dag.Operation
}

func (p *EnhancedFinal) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("🏁 Enhanced Final: Completing processing with enhanced features")
	var data []map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("Final Error: %s", err.Error()), Ctx: ctx}
	}
	for i, row := range data {
		row["done"] = true
		row["processed_by"] = "enhanced_dag"
		data[i] = row
	}
	updatedPayload, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}
