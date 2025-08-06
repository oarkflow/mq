package handlers

/*
Data Transformation Handlers Usage Examples

This file contains examples of how to configure and use the various data transformation handlers.
All configurations are done through the dag.Operation.Payload.Data map - no handler-specific configurations.

1. FORMAT HANDLER
=================
Supports: string, number, date, currency, uppercase, lowercase, capitalize, trim

Example configuration:
{
  "format_type": "uppercase",
  "fields": ["name", "title"],
  "currency": "$",
  "date_format": "2006-01-02"
}

2. GROUP HANDLER
================
Groups data with aggregation functions

Example configuration:
{
  "group_by": ["department", "status"],
  "aggregations": {
    "salary": "sum",
    "age": "avg",
    "count": "count",
    "name": "concat"
  },
  "concat_separator": ", "
}

3. SPLIT/JOIN HANDLER
====================
Handles string operations

Split example:
{
  "operation": "split",
  "fields": ["full_name"],
  "separator": " "
}

Join example:
{
  "operation": "join",
  "source_fields": ["first_name", "last_name"],
  "target_field": "full_name",
  "separator": " "
}

4. FLATTEN HANDLER
==================
Flattens nested data structures

Flatten settings example (key-value pairs):
{
  "operation": "flatten_settings",
  "source_field": "settings",
  "target_field": "config"
}

Input: {"settings": [{"key": "theme", "value": "dark", "value_type": "string"}]}
Output: {"config": {"theme": "dark"}}

5. JSON HANDLER
===============
JSON parsing and manipulation

Parse JSON string:
{
  "operation": "parse",
  "fields": ["json_data"]
}

Stringify object:
{
  "operation": "stringify",
  "fields": ["object_data"],
  "indent": true
}

6. FIELD HANDLER
================
Field manipulation operations

Filter fields:
{
  "operation": "filter",
  "fields": ["name", "email", "age"]
}

Rename fields:
{
  "operation": "rename",
  "mapping": {
    "old_name": "new_name",
    "email_addr": "email"
  }
}

Add fields:
{
  "operation": "add",
  "new_fields": {
    "created_at": "2023-01-01",
    "status": "active"
  }
}

Transform keys:
{
  "operation": "transform_keys",
  "transformation": "snake_case"  // or camel_case, kebab_case, etc.
}

7. DATA HANDLER
===============
Miscellaneous data operations

Sort data:
{
  "operation": "sort",
  "sort_field": "created_at",
  "sort_order": "desc"
}

Deduplicate:
{
  "operation": "deduplicate",
  "dedupe_fields": ["email", "phone"]
}

Calculate fields:
{
  "operation": "calculate",
  "calculations": {
    "total": {
      "operation": "sum",
      "fields": ["amount1", "amount2"]
    },
    "average_score": {
      "operation": "average",
      "fields": ["score1", "score2", "score3"]
    }
  }
}

Type casting:
{
  "operation": "type_cast",
  "cast": {
    "age": "int",
    "salary": "float",
    "active": "bool"
  }
}

Validate fields:
{
  "operation": "validate_fields",
  "validation_rules": {
    "email": {
      "required": true,
      "type": "string"
    },
    "age": {
      "required": true,
      "type": "int",
      "min": 0
    }
  }
}

USAGE IN DAG:
=============

import "github.com/oarkflow/mq/handlers"
import "github.com/oarkflow/mq/dag"

// Create handler
formatHandler := handlers.NewFormatHandler("format-1")

// Configure through Operation.Payload
config := dag.Payload{
    Data: map[string]any{
        "format_type": "uppercase",
        "fields": []string{"name", "title"},
    },
}
formatHandler.SetConfig(config)

// Use in DAG
dag := dag.NewDAG("data-processing")
dag.AddNode(formatHandler)

CHAINING OPERATIONS:
===================

You can chain multiple handlers in a DAG:
1. Parse JSON → 2. Flatten → 3. Filter fields → 4. Format → 5. Group

Each handler receives the output of the previous handler as input.
*/
