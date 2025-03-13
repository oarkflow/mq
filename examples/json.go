package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/jet"
	"github.com/oarkflow/json"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/dag"
)

type Data struct {
	Template string            `json:"template"`
	Mapping  map[string]string `json:"mapping"`
}

type Node struct {
	Name      string `json:"name"`
	ID        string `json:"id"`
	Node      string `json:"node"`
	Data      Data   `json:"data"`
	FirstNode bool   `json:"first_node"`
}

type Edge struct {
	Source string   `json:"source"`
	Target []string `json:"target"`
}

type Handler struct {
	Name  string `json:"name"`
	Key   string `json:"key"`
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

func CreateDAGFromJSON(config string) (*dag.DAG, error) {
	var handler Handler
	err := json.Unmarshal([]byte(config), &handler)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %v", err)
	}

	flow := dag.NewDAG(handler.Name, handler.Key, func(taskID string, result mq.Result) {
		fmt.Printf("Final result for task %s: %s\n", taskID, string(result.Payload))
	})

	nodeMap := make(map[string]mq.Processor)

	for _, node := range handler.Nodes {
		op := &HTMLProcessor{
			Template: node.Data.Template,
			Mapping:  node.Data.Mapping,
		}
		nodeMap[node.ID] = op
		flow.AddNode(dag.Page, node.Name, node.ID, op, node.FirstNode)
	}

	for _, edge := range handler.Edges {
		for _, target := range edge.Target {
			flow.AddEdge(dag.Simple, edge.Source, edge.Source, target)
		}
	}

	if flow.Error != nil {
		return nil, flow.Error
	}
	return flow, nil
}

type HTMLProcessor struct {
	dag.Operation
	Template string
	Mapping  map[string]string
}

func (p *HTMLProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	data := map[string]interface{}{
		"task_id": ctx.Value("task_id"),
	}

	for key, value := range p.Mapping {
		data[key] = value
	}

	rs, err := parser.ParseTemplate(p.Template, data)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	response := map[string]interface{}{
		"html_content": rs,
	}
	payload, _ := json.Marshal(response)

	return mq.Result{Payload: payload, Ctx: ctx}
}

func main() {
	// JSON configuration
	jsonConfig := `{
  "name": "Multi-Step Form",
  "key": "multi-step-form",
  "nodes": [
    {
      "name": "Form Step1",
      "id": "FormStep1",
      "node": "Page",
      "data": {
        "template": "<html>\n<body>\n<form method=\"post\" action=\"/process?task_id={{task_id}}&next=true\">\n    <label>Name:</label>\n    <input type=\"text\" name=\"name\" required>\n    <label>Age:</label>\n    <input type=\"number\" name=\"age\" required>\n    <button type=\"submit\">Next</button>\n</form>\n</body>\n</html>",
        "mapping": {}
      },
      "first_node": true
    },
    {
      "name": "Form Step2",
      "id": "FormStep2",
      "node": "Page",
      "data": {
        "template": "<html>\n<body>\n<form method=\"post\" action=\"/process?task_id={{task_id}}&next=true\">\n    {{ if show_voting_controls }}\n        <label>Do you want to register to vote?</label>\n        <input type=\"checkbox\" name=\"register_vote\">\n        <button type=\"submit\">Next</button>\n    {{ else }}\n        <p>You are not eligible to vote.</p>\n    {{ end }}\n</form>\n</body>\n</html>",
        "mapping": {
          "show_voting_controls": "conditional"
        }
      }
    },
    {
      "name": "Form Result",
      "id": "FormResult",
      "node": "Page",
      "data": {
        "template": "<html>\n<body>\n<h1>Form Summary</h1>\n<p>Name: {{ name }}</p>\n<p>Age: {{ age }}</p>\n{{ if register_vote }}\n    <p>You have registered to vote!</p>\n{{ else }}\n    <p>You did not register to vote.</p>\n{{ end }}\n</body>\n</html>",
        "mapping": {}
      }
    }
  ],
  "edges": [
    {
      "source": "FormStep1",
      "target": ["FormStep2"]
    },
    {
      "source": "FormStep2",
      "target": ["FormResult"]
    }
  ]
}`

	flow, err := CreateDAGFromJSON(jsonConfig)
	if err != nil {
		log.Fatalf("Error creating DAG: %v", err)
	}

	flow.Start(context.Background(), "0.0.0.0:8082")
}
