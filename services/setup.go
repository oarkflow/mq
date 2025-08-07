package services

import (
	"errors"
	"fmt"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/log"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

func SetupHandler(handler Handler, brokerAddr string, async ...bool) *dag.DAG {
	syncMode := true
	if len(async) > 0 {
		syncMode = async[0]
	}
	key := fmt.Sprintf(`%s-%v`, handler.Key, syncMode)
	existingDAG := dag.GetDAG(key)
	if existingDAG != nil {
		return existingDAG
	}
	flow := dag.NewDAG(handler.Name, handler.Key, nil, mq.WithSyncMode(syncMode), mq.WithBrokerURL(brokerAddr))
	for _, node := range handler.Nodes {
		err := prepareNode(flow, node)
		if err != nil {
			flow.Error = err
			return flow
		}
	}
	for _, edge := range handler.Edges {
		if edge.Label == "" {
			edge.Label = fmt.Sprintf("edge-%s", edge.Source)
		}
		flow.AddEdge(dag.Simple, edge.Label, edge.Source, edge.Target...)
		if flow.Error != nil {
			return flow
		}
	}
	for _, edge := range handler.Loops {
		if edge.Label == "" {
			edge.Label = fmt.Sprintf("loop-%s", edge.Source)
		}
		flow.AddEdge(dag.Iterator, edge.Label, edge.Source, edge.Target...)
	}
	err := flow.Validate()
	if err != nil {
		flow.Error = err
	}
	dag.AddDAG(key, flow)
	return flow
}

type Filter struct {
	Filter *filters.Filter `json:"condition"`
	Node   string          `json:"node"`
	ID     string          `json:"id"`
}

func prepareNode(flow *dag.DAG, node Node) error {
	newHandler := dag.GetHandler(node.Node)
	if newHandler == nil {
		return errors.New("Handler not found " + node.Node)
	}
	nodeHandler := newHandler(node.ID)
	providers := mapProviders(node.Data.Providers)
	switch nodeHandler := nodeHandler.(type) {
	case dag.ConditionProcessor:
		nodeHandler.SetConfig(dag.Payload{
			Mapping:         node.Data.Mapping,
			Data:            node.Data.AdditionalData,
			GeneratedFields: node.Data.GeneratedFields,
			Providers:       providers,
		})
		if s, ok := node.Data.AdditionalData["conditions"]; ok {
			var fil map[string]*Filter
			err := Map(&fil, s)
			if err != nil {
				return err
			}
			condition := make(map[string]string)
			conditions := make(map[string]dag.Condition)
			for key, cond := range fil {
				condition[key] = cond.Node
				conditions[key] = cond.Filter
			}
			flow.AddCondition(node.ID, condition)
			nodeHandler.SetConditions(conditions)
		}
	case dag.Processor:
		nodeHandler.SetConfig(dag.Payload{
			Mapping:         node.Data.Mapping,
			Data:            node.Data.AdditionalData,
			GeneratedFields: node.Data.GeneratedFields,
			Providers:       providers,
		})
	}
	var nodeType dag.NodeType
	if nodeHandler.GetType() == "Function" {
		nodeType = dag.Function
	} else if nodeHandler.GetType() == "Page" {
		nodeType = dag.Page
	}
	if node.Name == "" {
		node.Name = node.ID
	}
	flow.AddNode(nodeType, node.Name, node.ID, nodeHandler, node.FirstNode)
	return nil
}

func mapProviders(dataProviders interface{}) []dag.Provider {
	var providers []dag.Provider
	err := Map(&providers, dataProviders)
	if err != nil {
		log.Warn().Err(err).Msg("Unable to map providers")
	}
	return providers
}
