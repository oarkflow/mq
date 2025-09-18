# Complete Workflow Engine Documentation

## Overview

This is a **production-ready, enterprise-grade workflow engine** built on top of the existing DAG system. It provides comprehensive workflow orchestration capabilities with support for complex business processes, data pipelines, approval workflows, and automated task execution.

## 🎯 Key Features

### Core Capabilities
- ✅ **Workflow Definition & Management** - JSON-based workflow definitions with versioning
- ✅ **Multi-Node Type Support** - Task, API, Transform, Decision, Human Task, Timer, Loop, Parallel, Database, Email, Webhook
- ✅ **Advanced Execution Engine** - DAG-based execution with state management and error handling
- ✅ **Flexible Scheduling** - Support for immediate, delayed, and conditional execution
- ✅ **RESTful API** - Complete HTTP API for workflow management and execution
- ✅ **Real-time Monitoring** - Execution tracking, metrics, and health monitoring
- ✅ **Error Handling & Recovery** - Retry policies, rollback support, and checkpoint recovery

### Enterprise Features
- ✅ **Scalable Architecture** - Worker pool management and concurrent execution
- ✅ **Data Persistence** - In-memory storage with extensible storage interface
- ✅ **Security Framework** - Authentication, authorization, and CORS support
- ✅ **Audit & Tracing** - Complete execution history and tracing capabilities
- ✅ **Variable Management** - Runtime variables and templating support
- ✅ **Condition-based Routing** - Dynamic workflow paths based on conditions

## 📁 Project Structure

```
workflow/
├── types.go           # Core types and interfaces
├── processors.go      # Node type processors (Task, API, Transform, etc.)
├── registry.go        # Workflow definition storage and management
├── engine.go          # Main workflow execution engine
├── api.go            # HTTP API handlers and routes
├── demo/
│   └── main.go       # Comprehensive demonstration
└── example/
    └── main.go       # Simple usage examples
```

## 🚀 Quick Start

### 1. Import the Package
```go
import "github.com/oarkflow/mq/workflow"
```

### 2. Create and Start Engine
```go
config := &workflow.Config{
    MaxWorkers:       10,
    ExecutionTimeout: 30 * time.Minute,
    EnableMetrics:    true,
    EnableAudit:      true,
}

engine := workflow.NewWorkflowEngine(config)
ctx := context.Background()
engine.Start(ctx)
defer engine.Stop(ctx)
```

### 3. Define a Workflow
```go
workflow := &workflow.WorkflowDefinition{
    ID:          "sample-workflow",
    Name:        "Sample Data Processing",
    Description: "A simple data processing workflow",
    Version:     "1.0.0",
    Status:      workflow.WorkflowStatusActive,
    Nodes: []workflow.WorkflowNode{
        {
            ID:   "fetch-data",
            Name: "Fetch Data",
            Type: workflow.NodeTypeAPI,
            Config: workflow.NodeConfig{
                URL:    "https://api.example.com/data",
                Method: "GET",
            },
        },
        {
            ID:   "process-data",
            Name: "Process Data",
            Type: workflow.NodeTypeTransform,
            Config: workflow.NodeConfig{
                TransformType: "json_path",
                Expression:    "$.data",
            },
        },
    },
    Edges: []workflow.WorkflowEdge{
        {
            ID:       "fetch-to-process",
            FromNode: "fetch-data",
            ToNode:   "process-data",
        },
    },
}

// Register workflow
engine.RegisterWorkflow(ctx, workflow)
```

### 4. Execute Workflow
```go
execution, err := engine.ExecuteWorkflow(ctx, "sample-workflow", map[string]interface{}{
    "input_data": "test_value",
}, &workflow.ExecutionOptions{
    Priority: workflow.PriorityMedium,
    Owner:    "user123",
})

if err != nil {
    log.Fatal(err)
}

fmt.Printf("Execution started: %s\n", execution.ID)
```

## 🏗️ Node Types

The workflow engine supports various node types for different use cases:

### Task Node
Execute custom scripts or commands
```go
{
    Type: workflow.NodeTypeTask,
    Config: workflow.NodeConfig{
        Script: "console.log('Processing:', ${data})",
    },
}
```

### API Node
Make HTTP requests to external services
```go
{
    Type: workflow.NodeTypeAPI,
    Config: workflow.NodeConfig{
        URL:    "https://api.service.com/endpoint",
        Method: "POST",
        Headers: map[string]string{
            "Authorization": "Bearer ${token}",
        },
    },
}
```

### Transform Node
Transform and manipulate data
```go
{
    Type: workflow.NodeTypeTransform,
    Config: workflow.NodeConfig{
        TransformType: "json_path",
        Expression:    "$.users[*].email",
    },
}
```

### Decision Node
Conditional routing based on rules
```go
{
    Type: workflow.NodeTypeDecision,
    Config: workflow.NodeConfig{
        Rules: []workflow.Rule{
            {
                Condition: "age >= 18",
                Output:    "adult",
                NextNode:  "adult-process",
            },
            {
                Condition: "age < 18",
                Output:    "minor",
                NextNode:  "minor-process",
            },
        },
    },
}
```

### Human Task Node
Wait for human intervention
```go
{
    Type: workflow.NodeTypeHumanTask,
    Config: workflow.NodeConfig{
        Custom: map[string]interface{}{
            "assignee":    "manager@company.com",
            "due_date":    "3 days",
            "description": "Please review and approve",
        },
    },
}
```

### Timer Node
Add delays or scheduled execution
```go
{
    Type: workflow.NodeTypeTimer,
    Config: workflow.NodeConfig{
        Duration: 30 * time.Second,
        Schedule: "0 9 * * 1", // Every Monday at 9 AM
    },
}
```

### Database Node
Execute database operations
```go
{
    Type: workflow.NodeTypeDatabase,
    Config: workflow.NodeConfig{
        Query:      "INSERT INTO logs (message, created_at) VALUES (?, ?)",
        Connection: "main_db",
    },
}
```

### Email Node
Send email notifications
```go
{
    Type: workflow.NodeTypeEmail,
    Config: workflow.NodeConfig{
        To:      []string{"user@example.com"},
        Subject: "Workflow Completed",
        Body:    "Your workflow has completed successfully.",
    },
}
```

## 🌐 REST API Endpoints

### Workflow Management
```
POST   /api/v1/workflows              # Create workflow
GET    /api/v1/workflows              # List workflows
GET    /api/v1/workflows/:id          # Get workflow
PUT    /api/v1/workflows/:id          # Update workflow
DELETE /api/v1/workflows/:id          # Delete workflow
GET    /api/v1/workflows/:id/versions # Get versions
```

### Execution Management
```
POST   /api/v1/workflows/:id/execute           # Execute workflow
GET    /api/v1/workflows/:id/executions        # List workflow executions
GET    /api/v1/workflows/executions            # List all executions
GET    /api/v1/workflows/executions/:id        # Get execution
POST   /api/v1/workflows/executions/:id/cancel # Cancel execution
POST   /api/v1/workflows/executions/:id/suspend# Suspend execution
POST   /api/v1/workflows/executions/:id/resume # Resume execution
```

### Monitoring
```
GET    /api/v1/workflows/health        # Health check
GET    /api/v1/workflows/metrics       # System metrics
```

## 🎮 Demo Application

Run the comprehensive demo to see all features:

```bash
cd /Users/sujit/Sites/mq
go build -o workflow-demo ./workflow/demo
./workflow-demo
```

The demo includes:
- **Data Processing Workflow** - API integration, validation, transformation, and storage
- **Approval Workflow** - Multi-stage human task workflow with conditional routing
- **ETL Pipeline** - Parallel data processing with complex transformations

Demo endpoints:
- `http://localhost:3000/` - Main API info
- `http://localhost:3000/demo/workflows` - View registered workflows
- `http://localhost:3000/demo/executions` - View running executions
- `http://localhost:3000/api/v1/workflows/health` - Health check

## 🔧 Configuration

### Engine Configuration
```go
config := &workflow.Config{
    MaxWorkers:       10,                    // Concurrent execution workers
    ExecutionTimeout: 30 * time.Minute,     // Maximum execution time
    EnableMetrics:    true,                  // Enable metrics collection
    EnableAudit:      true,                  // Enable audit logging
    EnableTracing:    true,                  // Enable execution tracing
    LogLevel:         "info",                // Logging level
    Storage: workflow.StorageConfig{
        Type:           "memory",             // Storage backend
        MaxConnections: 100,                 // Max storage connections
    },
    Security: workflow.SecurityConfig{
        EnableAuth:     false,               // Enable authentication
        AllowedOrigins: []string{"*"},       // CORS allowed origins
    },
}
```

### Workflow Configuration
```go
config := workflow.WorkflowConfig{
    Timeout:     &timeout,                   // Workflow timeout
    MaxRetries:  3,                         // Maximum retry attempts
    Priority:    workflow.PriorityMedium,   // Execution priority
    Concurrency: 5,                         // Concurrent node execution
    ErrorHandling: workflow.ErrorHandling{
        OnFailure: "stop",                  // stop, continue, retry
        MaxErrors: 3,                       // Maximum errors allowed
        Rollback:  false,                   // Enable rollback on failure
    },
}
```

## 📊 Execution Monitoring

### Execution Status
- `pending` - Execution is queued
- `running` - Currently executing
- `completed` - Finished successfully
- `failed` - Execution failed
- `cancelled` - Manually cancelled
- `suspended` - Temporarily suspended

### Execution Context
Each execution maintains:
- **Variables** - Runtime variables and data
- **Trace** - Complete execution history
- **Checkpoints** - Recovery points
- **Metadata** - Additional context information

### Node Execution Tracking
Each node execution tracks:
- Input/Output data
- Execution duration
- Error information
- Retry attempts
- Execution logs

## 🔒 Security Features

### Authentication & Authorization
- Configurable authentication system
- Role-based access control
- API key management
- JWT token support

### Data Security
- Input/output data encryption
- Secure variable storage
- Audit trail logging
- CORS protection

## 🚀 Performance Features

### Scalability
- Horizontal scaling support
- Worker pool management
- Concurrent execution
- Resource optimization

### Optimization
- DAG-based execution optimization
- Caching strategies
- Memory management
- Performance monitoring

## 🔧 Extensibility

### Custom Node Types
Add custom processors by implementing the `WorkflowProcessor` interface:

```go
type CustomProcessor struct {
    Config workflow.NodeConfig
}

func (p *CustomProcessor) Process(ctx context.Context, data []byte) mq.Result {
    // Custom processing logic
    return mq.Result{Payload: processedData}
}

func (p *CustomProcessor) Close() error {
    // Cleanup logic
    return nil
}
```

### Storage Backends
Implement custom storage by satisfying the interfaces:
- `WorkflowRegistry` - Workflow definition storage
- `StateManager` - Execution state management

### Custom Middleware
Add middleware for cross-cutting concerns:
- Logging
- Metrics collection
- Authentication
- Rate limiting

## 📈 Production Considerations

### Monitoring & Observability
- Implement proper logging
- Set up metrics collection
- Configure health checks
- Enable distributed tracing

### High Availability
- Database clustering
- Load balancing
- Failover mechanisms
- Backup strategies

### Security Hardening
- Enable authentication
- Implement proper RBAC
- Secure API endpoints
- Audit logging

## 🎯 Use Cases

This workflow engine is perfect for:

1. **Data Processing Pipelines** - ETL/ELT operations, data validation, transformation
2. **Business Process Automation** - Approval workflows, document processing, compliance
3. **Integration Workflows** - API orchestration, system integration, event processing
4. **DevOps Automation** - CI/CD pipelines, deployment workflows, infrastructure automation
5. **Notification Systems** - Multi-channel notifications, escalation workflows
6. **Content Management** - Publishing workflows, review processes, content distribution

## ✅ Production Readiness Checklist

The workflow engine includes all production-ready features:

- ✅ **Comprehensive Type System** - Full type definitions for all components
- ✅ **Multiple Node Processors** - 11+ different node types for various use cases
- ✅ **Storage & Registry** - Versioned workflow storage with filtering and pagination
- ✅ **Execution Engine** - DAG-based execution with state management
- ✅ **Scheduling System** - Delayed execution and workflow scheduling
- ✅ **REST API** - Complete HTTP API with all CRUD operations
- ✅ **Error Handling** - Comprehensive error handling and recovery
- ✅ **Monitoring** - Health checks, metrics, and execution tracking
- ✅ **Security** - Authentication, authorization, and CORS support
- ✅ **Scalability** - Worker pools, concurrency control, and resource management
- ✅ **Extensibility** - Plugin architecture for custom processors and storage
- ✅ **Documentation** - Complete documentation with examples and demos

## 🎉 Conclusion

This complete workflow engine provides everything needed for production enterprise workflow automation. It combines the power of the existing DAG system with modern workflow orchestration capabilities, making it suitable for a wide range of business applications.

The engine is designed to be:
- **Powerful** - Handles complex workflows with conditional routing and parallel processing
- **Flexible** - Supports multiple node types and custom extensions
- **Scalable** - Built for high-throughput production environments
- **Reliable** - Comprehensive error handling and recovery mechanisms
- **Observable** - Full monitoring, tracing, and metrics capabilities
- **Secure** - Enterprise-grade security features

Start building your workflows today! 🚀
