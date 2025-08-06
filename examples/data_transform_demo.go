package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/handlers"
)

func main() {
	fmt.Println("=== Data Transformation Handlers Examples ===")

	// Test each handler with sample data
	testFormatHandler()
	testGroupHandler()
	testSplitJoinHandler()
	testFlattenHandler()
	testJSONHandler()
	testFieldHandler()
	testDataHandler()

	// Example of chaining handlers
	exampleDAGChaining()
}

func testFormatHandler() {
	fmt.Println("\n1. FORMAT HANDLER TESTS")
	fmt.Println("========================")

	// Test uppercase formatting
	testData := map[string]any{
		"name":  "john doe",
		"title": "software engineer",
		"age":   30,
	}

	handler := handlers.NewFormatHandler("format-test")
	config := dag.Payload{
		Data: map[string]any{
			"format_type": "uppercase",
			"fields":      []string{"name", "title"},
		},
	}
	handler.SetConfig(config)

	result := runHandler(handler, testData, "Uppercase Format")
	printResult("Uppercase formatting", result)
	printRequestConfigResult(testData, config, result)

	// Test currency formatting
	currencyData := map[string]any{
		"price": 99.99,
		"tax":   "15.50",
		"total": 115.49,
	}

	currencyHandler := handlers.NewFormatHandler("currency-test")
	currencyConfig := dag.Payload{
		Data: map[string]any{
			"format_type": "currency",
			"fields":      []string{"price", "tax", "total"},
			"currency":    "$",
		},
	}
	currencyHandler.SetConfig(currencyConfig)

	result = runHandler(currencyHandler, currencyData, "Currency Format")
	printResult("Currency formatting", result)
	printRequestConfigResult(currencyData, currencyConfig, result)

	// Test date formatting
	dateData := map[string]any{
		"created_at": "2023-06-15T10:30:00Z",
		"updated_at": "2023-06-20",
	}

	dateHandler := handlers.NewFormatHandler("date-test")
	dateConfig := dag.Payload{
		Data: map[string]any{
			"format_type": "date",
			"fields":      []string{"created_at", "updated_at"},
			"date_format": "2006-01-02",
		},
	}
	dateHandler.SetConfig(dateConfig)

	result = runHandler(dateHandler, dateData, "Date Format")
	printResult("Date formatting", result)
	printRequestConfigResult(dateData, dateConfig, result)
}

func testGroupHandler() {
	fmt.Println("\n2. GROUP HANDLER TESTS")
	fmt.Println("======================")

	// Test data grouping with aggregation
	testData := map[string]any{
		"data": []interface{}{
			map[string]any{"department": "Engineering", "salary": 80000, "age": 30, "name": "John"},
			map[string]any{"department": "Engineering", "salary": 90000, "age": 25, "name": "Jane"},
			map[string]any{"department": "Marketing", "salary": 60000, "age": 35, "name": "Bob"},
			map[string]any{"department": "Marketing", "salary": 65000, "age": 28, "name": "Alice"},
			map[string]any{"department": "Engineering", "salary": 95000, "age": 32, "name": "Mike"},
		},
	}

	handler := handlers.NewGroupHandler("group-test")
	config := dag.Payload{
		Data: map[string]any{
			"group_by": []string{"department"},
			"aggregations": map[string]any{
				"salary": "sum",
				"age":    "avg",
				"name":   "concat",
			},
			"concat_separator": ", ",
		},
	}
	handler.SetConfig(config)

	result := runHandler(handler, testData, "Group by Department")
	printResult("Data grouping", result)
	printRequestConfigResult(testData, config, result)
}

func testSplitJoinHandler() {
	fmt.Println("\n3. SPLIT/JOIN HANDLER TESTS")
	fmt.Println("============================")

	// Test split operation
	testData := map[string]any{
		"full_name": "John Michael Doe",
		"tags":      "go,programming,backend,api",
		"skills":    "golang python javascript",
	}

	splitHandler := handlers.NewSplitHandler("split-test")
	splitConfig := dag.Payload{
		Data: map[string]any{
			"fields":    []string{"full_name", "skills"},
			"separator": " ",
		},
	}
	splitHandler.SetConfig(splitConfig)

	result := runHandler(splitHandler, testData, "Split Operation (space)")
	printResult("String splitting with space", result)
	printRequestConfigResult(testData, splitConfig, result)

	// Test split with comma
	splitHandler2 := handlers.NewSplitHandler("split-test-2")
	splitConfig2 := dag.Payload{
		Data: map[string]any{
			"fields":    []string{"tags"},
			"separator": ",",
		},
	}
	splitHandler2.SetConfig(splitConfig2)

	result = runHandler(splitHandler2, testData, "Split Operation (comma)")
	printResult("String splitting with comma", result)
	printRequestConfigResult(testData, splitConfig2, result)

	// Test join operation
	joinData := map[string]any{
		"first_name":  "John",
		"middle_name": "Michael",
		"last_name":   "Doe",
		"title":       "Mr.",
	}

	joinHandler := handlers.NewJoinHandler("join-test")
	joinConfig := dag.Payload{
		Data: map[string]any{
			"source_fields": []string{"title", "first_name", "middle_name", "last_name"},
			"target_field":  "full_name_with_title",
			"separator":     " ",
		},
	}
	joinHandler.SetConfig(joinConfig)

	result = runHandler(joinHandler, joinData, "Join Operation")
	printResult("String joining", result)
	printRequestConfigResult(joinData, joinConfig, result)
}

func testFlattenHandler() {
	fmt.Println("\n4. FLATTEN HANDLER TESTS")
	fmt.Println("=========================")

	// Test flatten settings
	testData := map[string]any{
		"user_id": 123,
		"settings": []interface{}{
			map[string]any{"key": "theme", "value": "dark", "value_type": "string"},
			map[string]any{"key": "notifications", "value": "true", "value_type": "boolean"},
			map[string]any{"key": "max_items", "value": "50", "value_type": "integer"},
			map[string]any{"key": "timeout", "value": "30.5", "value_type": "float"},
		},
	}

	handler := handlers.NewFlattenHandler("flatten-test")
	config := dag.Payload{
		Data: map[string]any{
			"operation":    "flatten_settings",
			"source_field": "settings",
			"target_field": "user_config",
		},
	}
	handler.SetConfig(config)

	result := runHandler(handler, testData, "Flatten Settings")
	printResult("Settings flattening", result)
	printRequestConfigResult(testData, config, result)

	// Test flatten key-value pairs
	kvData := map[string]any{
		"user_id": 456,
		"properties": []interface{}{
			map[string]any{"name": "color", "val": "blue"},
			map[string]any{"name": "size", "val": "large"},
			map[string]any{"name": "weight", "val": "heavy"},
		},
	}

	kvHandler := handlers.NewFlattenHandler("kv-test")
	kvConfig := dag.Payload{
		Data: map[string]any{
			"operation":    "flatten_key_value",
			"source_field": "properties",
			"key_field":    "name",
			"value_field":  "val",
			"target_field": "flattened_props",
		},
	}
	kvHandler.SetConfig(kvConfig)

	result = runHandler(kvHandler, kvData, "Flatten Key-Value")
	printResult("Key-value flattening", result)
	printRequestConfigResult(kvData, kvConfig, result)

	// Test flatten nested objects
	nestedData := map[string]any{
		"user": map[string]any{
			"id": 123,
			"profile": map[string]any{
				"name":  "John Doe",
				"email": "john@example.com",
				"address": map[string]any{
					"street":  "123 Main St",
					"city":    "New York",
					"country": "USA",
				},
				"preferences": map[string]any{
					"theme":    "dark",
					"language": "en",
				},
			},
		},
	}

	nestedHandler := handlers.NewFlattenHandler("nested-test")
	nestedConfig := dag.Payload{
		Data: map[string]any{
			"operation": "flatten_nested_objects",
			"separator": "_",
		},
	}
	nestedHandler.SetConfig(nestedConfig)

	result = runHandler(nestedHandler, nestedData, "Flatten Nested Objects")
	printResult("Nested object flattening", result)
	printRequestConfigResult(nestedData, nestedConfig, result)
}

func testJSONHandler() {
	fmt.Println("\n5. JSON HANDLER TESTS")
	fmt.Println("=====================")

	// Test JSON parsing
	testData := map[string]any{
		"config":   `{"theme": "dark", "language": "en", "notifications": true, "max_items": 100}`,
		"metadata": `["tag1", "tag2", "tag3"]`,
		"user":     `{"id": 123, "name": "John Doe", "active": true}`,
	}

	parseHandler := handlers.NewJSONHandler("json-parse-test")
	parseConfig := dag.Payload{
		Data: map[string]any{
			"operation": "parse",
			"fields":    []string{"config", "metadata", "user"},
		},
	}
	parseHandler.SetConfig(parseConfig)

	result := runHandler(parseHandler, testData, "JSON Parsing")
	printResult("JSON parsing", result)
	printRequestConfigResult(testData, parseConfig, result)

	// Test JSON stringifying
	objData := map[string]any{
		"user": map[string]any{
			"id":     123,
			"name":   "John Doe",
			"active": true,
			"roles":  []string{"admin", "user"},
		},
		"preferences": map[string]any{
			"theme":         "dark",
			"notifications": true,
			"language":      "en",
		},
	}

	stringifyHandler := handlers.NewJSONHandler("json-stringify-test")
	stringifyConfig := dag.Payload{
		Data: map[string]any{
			"operation": "stringify",
			"fields":    []string{"user", "preferences"},
			"indent":    true,
		},
	}
	stringifyHandler.SetConfig(stringifyConfig)

	result = runHandler(stringifyHandler, objData, "JSON Stringifying")
	printResult("JSON stringifying", result)
	printRequestConfigResult(objData, stringifyConfig, result)

	// Test JSON validation
	validationData := map[string]any{
		"valid_json":   `{"key": "value"}`,
		"invalid_json": `{"key": value}`, // Missing quotes around value
		"valid_array":  `[1, 2, 3]`,
	}

	validateHandler := handlers.NewJSONHandler("json-validate-test")
	validateConfig := dag.Payload{
		Data: map[string]any{
			"operation": "validate",
			"fields":    []string{"valid_json", "invalid_json", "valid_array"},
		},
	}
	validateHandler.SetConfig(validateConfig)

	result = runHandler(validateHandler, validationData, "JSON Validation")
	printResult("JSON validation", result)
	printRequestConfigResult(validationData, validateConfig, result)
}

func testFieldHandler() {
	fmt.Println("\n6. FIELD HANDLER TESTS")
	fmt.Println("======================")

	testData := map[string]any{
		"id":           123,
		"first_name":   "John",
		"last_name":    "Doe",
		"email_addr":   "john@example.com",
		"phone_number": "555-1234",
		"internal_id":  "INT-123",
		"created_at":   "2023-01-15",
		"updated_at":   "2023-06-20",
		"is_active":    true,
		"salary":       75000.50,
	}

	// Test field filtering/selection
	filterHandler := handlers.NewFieldHandler("filter-test")
	filterConfig := dag.Payload{
		Data: map[string]any{
			"operation": "filter",
			"fields":    []string{"id", "first_name", "last_name", "email_addr", "is_active"},
		},
	}
	filterHandler.SetConfig(filterConfig)

	result := runHandler(filterHandler, testData, "Filter/Select Fields")
	printResult("Field filtering", result)
	printRequestConfigResult(testData, filterConfig, result)

	// Test field exclusion/removal
	excludeHandler := handlers.NewFieldHandler("exclude-test")
	excludeConfig := dag.Payload{
		Data: map[string]any{
			"operation": "exclude",
			"fields":    []string{"internal_id", "created_at", "updated_at"},
		},
	}
	excludeHandler.SetConfig(excludeConfig)

	result = runHandler(excludeHandler, testData, "Exclude Fields")
	printResult("Field exclusion", result)
	printRequestConfigResult(testData, excludeConfig, result)

	// Test field renaming
	renameHandler := handlers.NewFieldHandler("rename-test")
	renameConfig := dag.Payload{
		Data: map[string]any{
			"operation": "rename",
			"mapping": map[string]any{
				"first_name":   "firstName",
				"last_name":    "lastName",
				"email_addr":   "email",
				"phone_number": "phone",
				"created_at":   "createdAt",
				"updated_at":   "updatedAt",
				"is_active":    "active",
			},
		},
	}
	renameHandler.SetConfig(renameConfig)

	result = runHandler(renameHandler, testData, "Rename Fields")
	printResult("Field renaming", result)
	printRequestConfigResult(testData, renameConfig, result)

	// Test adding new fields
	addHandler := handlers.NewFieldHandler("add-test")
	addConfig := dag.Payload{
		Data: map[string]any{
			"operation": "add",
			"new_fields": map[string]any{
				"status":       "active",
				"version":      "1.0",
				"is_verified":  true,
				"last_login":   "2023-06-20T10:30:00Z",
				"department":   "Engineering",
				"access_level": 3,
			},
		},
	}
	addHandler.SetConfig(addConfig)

	result = runHandler(addHandler, testData, "Add Fields")
	printResult("Adding fields", result)
	printRequestConfigResult(testData, addConfig, result)

	// Test field copying
	copyHandler := handlers.NewFieldHandler("copy-test")
	copyConfig := dag.Payload{
		Data: map[string]any{
			"operation": "copy",
			"mapping": map[string]any{
				"first_name": "display_name",
				"email_addr": "contact_email",
				"id":         "user_id",
			},
		},
	}
	copyHandler.SetConfig(copyConfig)

	result = runHandler(copyHandler, testData, "Copy Fields")
	printResult("Field copying", result)
	printRequestConfigResult(testData, copyConfig, result)

	// Test key transformation
	transformHandler := handlers.NewFieldHandler("transform-test")
	transformConfig := dag.Payload{
		Data: map[string]any{
			"operation":      "transform_keys",
			"transformation": "snake_case",
		},
	}
	transformHandler.SetConfig(transformConfig)

	result = runHandler(transformHandler, testData, "Transform Keys")
	printResult("Key transformation", result)
	printRequestConfigResult(testData, transformConfig, result)
}

func testDataHandler() {
	fmt.Println("\n7. DATA HANDLER TESTS")
	fmt.Println("=====================")

	// Test data sorting
	testData := map[string]any{
		"data": []interface{}{
			map[string]any{"name": "John", "age": 30, "salary": 80000, "department": "Engineering"},
			map[string]any{"name": "Jane", "age": 25, "salary": 90000, "department": "Engineering"},
			map[string]any{"name": "Bob", "age": 35, "salary": 75000, "department": "Marketing"},
			map[string]any{"name": "Alice", "age": 28, "salary": 85000, "department": "Marketing"},
		},
	}

	sortHandler := handlers.NewDataHandler("sort-test")
	sortConfig := dag.Payload{
		Data: map[string]any{
			"operation":  "sort",
			"sort_field": "salary",
			"sort_order": "desc",
		},
	}
	sortHandler.SetConfig(sortConfig)

	result := runHandler(sortHandler, testData, "Sort Data by Salary (Desc)")
	printResult("Data sorting", result)
	printRequestConfigResult(testData, sortConfig, result)

	// Test field calculations
	calcData := map[string]any{
		"base_price":    100.0,
		"tax_rate":      0.15,
		"shipping_cost": 10.0,
		"discount":      5.0,
		"quantity":      2,
	}

	calcHandler := handlers.NewDataHandler("calc-test")
	calcConfig := dag.Payload{
		Data: map[string]any{
			"operation": "calculate",
			"calculations": map[string]any{
				"tax_amount": map[string]any{
					"operation": "multiply",
					"fields":    []string{"base_price", "tax_rate"},
				},
				"subtotal": map[string]any{
					"operation": "sum",
					"fields":    []string{"base_price", "tax_amount", "shipping_cost"},
				},
				"total": map[string]any{
					"operation": "subtract",
					"fields":    []string{"subtotal", "discount"},
				},
				"grand_total": map[string]any{
					"operation": "multiply",
					"fields":    []string{"total", "quantity"},
				},
			},
		},
	}
	calcHandler.SetConfig(calcConfig)

	result = runHandler(calcHandler, calcData, "Field Calculations")
	printResult("Field calculations", result)
	printRequestConfigResult(calcData, calcConfig, result)

	// Test data deduplication
	dupData := map[string]any{
		"data": []interface{}{
			map[string]any{"email": "john@example.com", "name": "John Doe", "id": 1},
			map[string]any{"email": "jane@example.com", "name": "Jane Smith", "id": 2},
			map[string]any{"email": "john@example.com", "name": "John D.", "id": 3}, // duplicate email
			map[string]any{"email": "bob@example.com", "name": "Bob Jones", "id": 4},
			map[string]any{"email": "jane@example.com", "name": "Jane S.", "id": 5}, // duplicate email
		},
	}

	dedupHandler := handlers.NewDataHandler("dedup-test")
	dedupConfig := dag.Payload{
		Data: map[string]any{
			"operation":     "deduplicate",
			"dedupe_fields": []string{"email"},
		},
	}
	dedupHandler.SetConfig(dedupConfig)

	result = runHandler(dedupHandler, dupData, "Data Deduplication")
	printResult("Data deduplication", result)
	printRequestConfigResult(dupData, dedupConfig, result)

	// Test type casting
	castData := map[string]any{
		"user_id":     "123",
		"age":         "30",
		"salary":      "75000.50",
		"is_active":   "true",
		"score":       "95.5",
		"name":        123,
		"is_verified": "false",
	}

	castHandler := handlers.NewDataHandler("cast-test")
	castConfig := dag.Payload{
		Data: map[string]any{
			"operation": "type_cast",
			"cast": map[string]any{
				"user_id":     "int",
				"age":         "int",
				"salary":      "float",
				"is_active":   "bool",
				"score":       "float",
				"name":        "string",
				"is_verified": "bool",
			},
		},
	}
	castHandler.SetConfig(castConfig)

	result = runHandler(castHandler, castData, "Type Casting")
	printResult("Type casting", result)
	printRequestConfigResult(castData, castConfig, result)

	// Test conditional field setting
	condData := map[string]any{
		"age":              25,
		"salary":           60000,
		"years_experience": 3,
	}

	condHandler := handlers.NewDataHandler("conditional-test")
	condConfig := dag.Payload{
		Data: map[string]any{
			"operation": "conditional_set",
			"conditions": map[string]any{
				"salary_level": map[string]any{
					"condition": "salary > 70000",
					"if_true":   "high",
					"if_false":  "standard",
				},
				"experience_level": map[string]any{
					"condition": "years_experience >= 5",
					"if_true":   "senior",
					"if_false":  "junior",
				},
			},
		},
	}
	condHandler.SetConfig(condConfig)

	result = runHandler(condHandler, condData, "Conditional Field Setting")
	printResult("Conditional setting", result)
	printRequestConfigResult(condData, condConfig, result)
}

// Helper functions
func runHandler(handler dag.Processor, data map[string]any, description string) map[string]any {
	fmt.Printf("\n--- Testing: %s ---\n", description)

	// Convert data to JSON payload
	payload, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshaling test data: %v", err)
		return nil
	}

	// Create a task
	task := &mq.Task{
		ID:      mq.NewID(),
		Payload: payload,
	}

	// Process the task
	ctx := context.Background()
	result := handler.ProcessTask(ctx, task)

	if result.Error != nil {
		log.Printf("Handler error: %v", result.Error)
		return nil
	}

	// Parse result payload
	var resultData map[string]any
	if err := json.Unmarshal(result.Payload, &resultData); err != nil {
		log.Printf("Error unmarshaling result: %v", err)
		return nil
	}

	return resultData
}

func printResult(operation string, result map[string]any) {
	if result == nil {
		fmt.Printf("❌ %s failed\n", operation)
		return
	}

	fmt.Printf("✅ %s succeeded\n", operation)

	// Pretty print the result (truncated for readability)
	resultJSON, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Printf("Error formatting result: %v\n", err)
		return
	}

	// Truncate very long results
	resultStr := string(resultJSON)
	if len(resultStr) > 1000 {
		resultStr = resultStr[:997] + "..."
	}
}

func printRequestConfigResult(requestData map[string]any, config dag.Payload, result map[string]any) {
	fmt.Println("\n=== Request Data ===")
	requestJSON, err := json.MarshalIndent(requestData, "", "  ")
	if err != nil {
		fmt.Printf("Error formatting request data: %v\n", err)
	} else {
		fmt.Println(string(requestJSON))
	}

	fmt.Println("\n=== Configuration ===")
	configJSON, err := json.MarshalIndent(config.Data, "", "  ")
	if err != nil {
		fmt.Printf("Error formatting configuration: %v\n", err)
	} else {
		fmt.Println(string(configJSON))
	}

	fmt.Println("\n=== Result ===")
	if result == nil {
		fmt.Println("❌ Operation failed")
	} else {
		resultJSON, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			fmt.Printf("Error formatting result: %v\n", err)
		} else {
			fmt.Println(string(resultJSON))
		}
	}
}

// Example of chaining handlers in a DAG workflow
func exampleDAGChaining() {
	fmt.Println("\n=== CHAINING HANDLERS EXAMPLE ===")
	fmt.Println("==================================")

	// Sample input data with nested JSON and various formatting needs
	inputData := map[string]any{
		"user_data": `{"firstName": "john", "lastName": "doe", "age": "30", "salary": "75000.50", "isActive": "true"}`,
		"metadata":  `{"department": "engineering", "level": "senior", "skills": ["go", "python", "javascript"]}`,
	}

	fmt.Println("🔗 Chaining multiple handlers to transform data...")
	fmt.Printf("Input data: %+v\n", inputData)

	// Step 1: Parse JSON strings
	jsonHandler := handlers.NewJSONHandler("json-step")
	jsonConfig := dag.Payload{
		Data: map[string]any{
			"operation": "parse",
			"fields":    []string{"user_data", "metadata"},
		},
	}
	jsonHandler.SetConfig(jsonConfig)

	step1Result := runHandler(jsonHandler, inputData, "Step 1: Parse JSON strings")

	if step1Result != nil {
		// Step 2: Flatten the parsed nested data
		flattenHandler := handlers.NewFlattenHandler("flatten-step")
		flattenConfig := dag.Payload{
			Data: map[string]any{
				"operation": "flatten_nested_objects",
				"separator": "_",
			},
		}
		flattenHandler.SetConfig(flattenConfig)

		step2Result := runHandler(flattenHandler, step1Result, "Step 2: Flatten nested objects")

		if step2Result != nil {
			// Step 3: Format name fields to proper case
			formatHandler := handlers.NewFormatHandler("format-step")
			formatConfig := dag.Payload{
				Data: map[string]any{
					"format_type": "capitalize",
					"fields":      []string{"user_data_parsed_firstName", "user_data_parsed_lastName"},
				},
			}
			formatHandler.SetConfig(formatConfig)

			step3Result := runHandler(formatHandler, step2Result, "Step 3: Format names to proper case")

			if step3Result != nil {
				// Step 4: Rename fields to standard naming
				fieldHandler := handlers.NewFieldHandler("rename-step")
				renameConfig := dag.Payload{
					Data: map[string]any{
						"operation": "rename",
						"mapping": map[string]any{
							"user_data_parsed_firstName": "first_name",
							"user_data_parsed_lastName":  "last_name",
							"user_data_parsed_age":       "age",
							"user_data_parsed_salary":    "salary",
							"user_data_parsed_isActive":  "is_active",
							"metadata_parsed_department": "department",
							"metadata_parsed_level":      "level",
						},
					},
				}
				fieldHandler.SetConfig(renameConfig)

				step4Result := runHandler(fieldHandler, step3Result, "Step 4: Rename fields")

				if step4Result != nil {
					// Step 5: Cast data types
					dataHandler := handlers.NewDataHandler("cast-step")
					castConfig := dag.Payload{
						Data: map[string]any{
							"operation": "type_cast",
							"cast": map[string]any{
								"age":       "int",
								"salary":    "float",
								"is_active": "bool",
							},
						},
					}
					dataHandler.SetConfig(castConfig)

					finalResult := runHandler(dataHandler, step4Result, "Step 5: Cast data types")
					printResult("🎉 Final chained transformation result", finalResult)
				}
			}
		}
	}
}
