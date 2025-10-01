package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/oarkflow/json"
	v2 "github.com/oarkflow/mq/dag/v2"
)

// ---------------------- Example Processors ----------------------

func doubleProc(ctx context.Context, in any) (any, error) {
	switch v := in.(type) {
	case int:
		return v * 2, nil
	case float64:
		return v * 2, nil
	default:
		return nil, errors.New("unsupported type for double")
	}
}

func incProc(ctx context.Context, in any) (any, error) {
	if n, ok := in.(int); ok {
		return n + 1, nil
	}
	return nil, errors.New("inc: not int")
}

func printProc(ctx context.Context, in any) (any, error) {
	fmt.Printf("OUTPUT: %#v\n", in)
	return in, nil
}

func getDataProc(ctx context.Context, in any) (any, error) {
	return in, nil
}

func loopProc(ctx context.Context, in any) (any, error) {
	return in, nil
}

func validateAgeProc(ctx context.Context, in any) (any, error) {
	m, ok := in.(map[string]any)
	if !ok {
		return nil, errors.New("not map")
	}
	age, ok := m["age"].(float64)
	if !ok {
		return nil, errors.New("no age")
	}
	status := "default"
	if age >= 18 {
		status = "pass"
	}
	m["condition_status"] = status
	return m, nil
}

func validateGenderProc(ctx context.Context, in any) (any, error) {
	m, ok := in.(map[string]any)
	if !ok {
		return nil, errors.New("not map")
	}
	gender, ok := m["gender"].(string)
	if !ok {
		return nil, errors.New("no gender")
	}
	m["female_voter"] = gender == "female"
	return m, nil
}

func finalProc(ctx context.Context, in any) (any, error) {
	m, ok := in.(map[string]any)
	if !ok {
		return nil, errors.New("not map")
	}
	m["done"] = true
	return m, nil
}

// ---------------------- Main Demo ----------------------

func pageFlow() {

}

func main() {
	v2.RegisterProcessor("double", doubleProc)
	v2.RegisterProcessor("inc", incProc)
	v2.RegisterProcessor("print", printProc)
	v2.RegisterProcessor("getData", getDataProc)
	v2.RegisterProcessor("loop", loopProc)
	v2.RegisterProcessor("validateAge", validateAgeProc)
	v2.RegisterProcessor("validateGender", validateGenderProc)
	v2.RegisterProcessor("final", finalProc)

	jsonSpec := `{
		"nodes": [
			{"id":"getData","type":"channel","processor":"getData"},
			{"id":"loop","type":"channel","processor":"loop"},
			{"id":"validateAge","type":"channel","processor":"validateAge"},
			{"id":"validateGender","type":"channel","processor":"validateGender"},
			{"id":"final","type":"channel","processor":"final"}
		],
		"edges": [
			{"source":"getData","targets":["loop"],"type":"simple"},
			{"source":"loop","targets":["validateAge"],"type":"iterator"},
			{"source":"validateGender","targets":["final"],"type":"simple"}
		],
		"entry_ids":["getData"],
		"conditions": {
			"validateAge": {"pass": "validateGender", "default": "final"}
		}
	}`

	var spec v2.PipelineSpec
	if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
		panic(err)
	}
	dag, err := v2.BuildDAGFromSpec(spec)
	if err != nil {
		panic(err)
	}

	in := make(chan any)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	out, err := dag.Start(ctx, in)
	if err != nil {
		panic(err)
	}

	go func() {
		data := []any{
			map[string]any{"age": 15.0, "gender": "female"},
			map[string]any{"age": 18.0, "gender": "male"},
		}
		in <- data
		close(in)
	}()

	var results []any
	for r := range out {
		results = append(results, r)
	}

	fmt.Println("Final results:", results)
	fmt.Println("pipeline finished")
}
