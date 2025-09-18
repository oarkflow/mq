# Enhanced Services with DAG + Workflow Engine

## Overview

The enhanced services architecture successfully integrates all workflow engine features into the DAG system, providing complete feature parity and backward compatibility. This upgrade provides both traditional DAG functionality and advanced workflow capabilities through a unified service layer.

## Architecture Components

### 1. Enhanced Service Manager (`enhanced_setup.go`)
- **Purpose**: Core service orchestration with DAG + workflow integration
- **Features**:
  - Dual-mode execution (Traditional DAG + Enhanced Workflow)
  - HTTP API endpoints for workflow management
  - Enhanced validation with workflow rule support
  - Service health monitoring and metrics
  - Background task management

### 2. Enhanced Contracts (`enhanced_contracts.go`)
- **Purpose**: Service interfaces for DAG + workflow integration
- **Key Interfaces**:
  - `EnhancedServiceManager`: Core service management
  - `EnhancedDAGService`: Dual-mode DAG operations
  - `EnhancedValidation`: Workflow validation rules
  - `EnhancedHandler`: Unified handler structure

### 3. Enhanced DAG Service (`enhanced_dag_service.go`)
- **Purpose**: DAG service with workflow engine capabilities
- **Features**:
  - Traditional DAG execution (backward compatibility)
  - Enhanced workflow execution with advanced processors
  - State management and persistence
  - Execution result handling with proper field mapping

### 4. Enhanced Validation (`enhanced_validation.go`)
- **Purpose**: Validation service with workflow rule support
- **Features**:
  - Schema validation with workflow rules
  - Field-level validation (string, email, numeric, etc.)
  - Custom validation logic with processor integration
  - Validation result aggregation

## Features Implemented

### Complete Workflow Engine Integration ✅
All 8 advanced processors from the workflow engine are now available in the DAG system:

1. **Validator Processor**: Schema and field validation
2. **Router Processor**: Conditional routing and decision making
3. **Transformer Processor**: Data transformation and mapping
4. **Aggregator Processor**: Data aggregation and summarization
5. **Filter Processor**: Data filtering and selection
6. **Sorter Processor**: Data sorting and ordering
7. **Notify Processor**: Notification and messaging
8. **Storage Processor**: Data persistence and retrieval

### Enhanced DAG Capabilities ✅
- **Dual Mode Support**: Both traditional DAG and workflow modes
- **Advanced Retry Logic**: Exponential backoff with circuit breaker
- **State Management**: Persistent execution state tracking
- **Scheduling**: Background task scheduling and execution
- **Security**: Authentication and authorization support
- **Middleware**: Pre/post execution hooks
- **Metrics**: Performance monitoring and reporting

### HTTP API Integration ✅
Complete REST API for workflow management:
- `GET /api/v1/handlers` - List all handlers
- `POST /api/v1/execute/:key` - Execute workflow by key
- `GET /api/v1/workflows` - List workflow instances
- `POST /api/v1/workflows/:id/execute` - Execute specific workflow
- `GET /health` - Service health check

### Validation System ✅
Enhanced validation with workflow rule support:
- Field-level validation rules
- Type checking (string, email, numeric, etc.)
- Length constraints (min/max)
- Required field validation
- Custom validation messages
- Validation result aggregation

## Usage Examples

### 1. Traditional DAG Mode (Backward Compatibility)
```go
// Traditional DAG handler
handler := services.EnhancedHandler{
    Key:             "traditional-dag",
    Name:            "Traditional DAG",
    WorkflowEnabled: false, // Use traditional DAG mode
    Nodes: []services.EnhancedNode{
        {
            ID:        "start",
            Name:      "Start Process",
            Node:      "basic",
            FirstNode: true,
        },
        {
            ID:   "process",
            Name: "Process Data",
            Node: "basic",
        },
    },
    Edges: []services.Edge{
        {Source: "start", Target: []string{"process"}},
    },
}
```

### 2. Enhanced Workflow Mode
```go
// Enhanced workflow handler with processors
handler := services.EnhancedHandler{
    Key:             "enhanced-workflow",
    Name:            "Enhanced Workflow",
    WorkflowEnabled: true, // Use enhanced workflow mode
    ValidationRules: []*dag.WorkflowValidationRule{
        {
            Field:     "email",
            Type:      "email",
            Required:  true,
            Message:   "Valid email is required",
        },
    },
    Nodes: []services.EnhancedNode{
        {
            ID:            "validate-input",
            Name:          "Validate Input",
            Type:          "validator",
            ProcessorType: "validator",
        },
        {
            ID:            "route-data",
            Name:          "Route Decision",
            Type:          "router",
            ProcessorType: "router",
        },
        {
            ID:            "transform-data",
            Name:          "Transform Data",
            Type:          "transformer",
            ProcessorType: "transformer",
        },
    },
    Edges: []services.Edge{
        {Source: "validate-input", Target: []string{"route-data"}},
        {Source: "route-data", Target: []string{"transform-data"}},
    },
}
```

### 3. Service Configuration
```go
config := &services.EnhancedServiceConfig{
    BrokerURL: "nats://localhost:4222",
    Debug:     true,

    // Enhanced DAG configuration
    EnhancedDAGConfig: &dag.EnhancedDAGConfig{
        EnableWorkflowEngine:    true,
        MaintainDAGMode:         true,
        EnableStateManagement:   true,
        EnableAdvancedRetry:     true,
        EnableCircuitBreaker:    true,
        MaxConcurrentExecutions: 10,
        DefaultTimeout:          30 * time.Second,
    },

    // Workflow engine configuration
    WorkflowEngineConfig: &dag.WorkflowEngineConfig{
        MaxConcurrentExecutions: 5,
        DefaultTimeout:          2 * time.Minute,
        EnablePersistence:       true,
        EnableSecurity:          true,
        RetryConfig: &dag.RetryConfig{
            MaxRetries:    3,
            InitialDelay:  1 * time.Second,
            BackoffFactor: 2.0,
        },
    },
}
```

### 4. Service Initialization
```go
// Create enhanced service manager
manager := services.NewEnhancedServiceManager(config)

// Initialize services
if err := manager.Initialize(config); err != nil {
    log.Fatalf("Failed to initialize services: %v", err)
}

// Start services
ctx := context.Background()
if err := manager.Start(ctx); err != nil {
    log.Fatalf("Failed to start services: %v", err)
}
defer manager.Stop(ctx)

// Register handlers
for _, handler := range handlers {
    if err := manager.RegisterEnhancedHandler(handler); err != nil {
        log.Printf("Failed to register handler %s: %v", handler.Key, err)
    }
}
```

### 5. HTTP API Setup
```go
// Create Fiber app
app := fiber.New()

// Register HTTP routes
if err := manager.RegisterHTTPRoutes(app); err != nil {
    log.Fatalf("Failed to register HTTP routes: %v", err)
}

// Start server
log.Fatal(app.Listen(":3000"))
```

### 6. Workflow Execution
```go
// Execute workflow programmatically
ctx := context.Background()
input := map[string]any{
    "name":  "John Doe",
    "email": "john@example.com",
}

result, err := manager.ExecuteEnhancedWorkflow(ctx, "enhanced-workflow", input)
if err != nil {
    log.Printf("Execution failed: %v", err)
} else {
    log.Printf("Execution completed: %s (Status: %s)", result.ID, result.Status)
}
```

## HTTP API Usage

### Execute Workflow via REST API
```bash
# Execute workflow with POST request
curl -X POST http://localhost:3000/api/v1/execute/enhanced-workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }'
```

### List Available Handlers
```bash
# Get list of registered handlers
curl -X GET http://localhost:3000/api/v1/handlers
```

### Health Check
```bash
# Check service health
curl -X GET http://localhost:3000/health
```

## Advanced Features

### 1. Validation Rules
The enhanced validation system supports comprehensive field validation:

```go
ValidationRules: []*dag.WorkflowValidationRule{
    {
        Field:     "name",
        Type:      "string",
        Required:  true,
        MinLength: 2,
        MaxLength: 50,
        Message:   "Name must be 2-50 characters",
    },
    {
        Field:    "email",
        Type:     "email",
        Required: true,
        Message:  "Valid email is required",
    },
    {
        Field:   "age",
        Type:    "number",
        Min:     18,
        Max:     120,
        Message: "Age must be between 18 and 120",
    },
}
```

### 2. Processor Configuration
Each processor can be configured with specific parameters:

```go
Config: dag.WorkflowNodeConfig{
    // Validator processor config
    ValidationType: "schema",
    ValidationRules: []dag.WorkflowValidationRule{...},

    // Router processor config
    RoutingRules: []dag.RoutingRule{...},

    // Transformer processor config
    TransformationRules: []dag.TransformationRule{...},

    // Storage processor config
    StorageType: "memory",
    StorageConfig: map[string]any{...},
}
```

### 3. Error Handling and Retry
Built-in retry logic with exponential backoff:

```go
RetryConfig: &dag.RetryConfig{
    MaxRetries:    3,
    InitialDelay:  1 * time.Second,
    MaxDelay:      30 * time.Second,
    BackoffFactor: 2.0,
}
```

### 4. State Management
Persistent execution state tracking:

```go
EnhancedDAGConfig: &dag.EnhancedDAGConfig{
    EnableStateManagement: true,
    EnablePersistence:     true,
}
```

## Migration Guide

### From Traditional DAG to Enhanced Services

1. **Keep existing DAG handlers**: Set `WorkflowEnabled: false`
2. **Add enhanced features gradually**: Create new handlers with `WorkflowEnabled: true`
3. **Use validation rules**: Add `ValidationRules` for input validation
4. **Configure processors**: Set appropriate `ProcessorType` for each node
5. **Test both modes**: Verify traditional and enhanced workflows work correctly

### Configuration Migration
```go
// Before (traditional)
config := &services.ServiceConfig{
    BrokerURL: "nats://localhost:4222",
}

// After (enhanced)
config := &services.EnhancedServiceConfig{
    BrokerURL: "nats://localhost:4222",
    EnhancedDAGConfig: &dag.EnhancedDAGConfig{
        EnableWorkflowEngine: true,
        MaintainDAGMode:     true, // Keep backward compatibility
    },
}
```

## Performance Considerations

1. **Concurrent Executions**: Configure `MaxConcurrentExecutions` based on system resources
2. **Timeout Settings**: Set appropriate `DefaultTimeout` for workflow complexity
3. **Retry Strategy**: Balance retry attempts with system load
4. **State Management**: Enable persistence only when needed
5. **Metrics**: Monitor performance with built-in metrics

## Troubleshooting

### Common Issues

1. **Handler Registration Fails**
   - Check validation rules syntax
   - Verify processor types are valid
   - Ensure node dependencies are correct

2. **Workflow Execution Errors**
   - Validate input data format
   - Check processor configurations
   - Review error logs for details

3. **HTTP API Issues**
   - Verify routes are registered correctly
   - Check request format and headers
   - Review service health status

### Debug Mode
Enable debug mode for detailed logging:
```go
config := &services.EnhancedServiceConfig{
    Debug: true,
    // ... other config
}
```

## Conclusion

The enhanced services architecture successfully provides complete feature parity between the DAG system and workflow engine. All workflow engine features are now available in the DAG system while maintaining full backward compatibility with existing traditional DAG implementations.

Key achievements:
- ✅ Complete workflow engine integration (8 advanced processors)
- ✅ Dual-mode support (traditional DAG + enhanced workflow)
- ✅ HTTP API for workflow management
- ✅ Enhanced validation with workflow rules
- ✅ Service health monitoring and metrics
- ✅ Backward compatibility maintained
- ✅ Production-ready architecture

The system now provides a unified, powerful, and flexible platform for both simple DAG operations and complex workflow orchestration.
