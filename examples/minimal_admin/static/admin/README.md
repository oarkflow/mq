# MQ Admin Dashboard

The MQ Admin Dashboard provides a comprehensive web-based interface for managing and monitoring your MQ broker, queues, consumers, and worker pools. It offers real-time metrics, control capabilities, and health monitoring similar to RabbitMQ's management interface.

## Features

### 🌟 **Comprehensive Dashboard**
- **Real-time Monitoring**: Live charts showing throughput, queue depth, and system metrics
- **Broker Management**: Monitor broker status, uptime, and configuration
- **Queue Management**: View queue depths, consumer counts, and flush capabilities
- **Consumer Control**: Monitor consumer status, pause/resume operations, and performance metrics
- **Worker Pool Management**: Track pool status, worker counts, and memory usage
- **Health Checks**: System health monitoring with detailed status reports

### 📊 **Real-time Visualizations**
- Interactive charts powered by Chart.js
- Live WebSocket updates for real-time data
- Throughput history and trend analysis
- System resource monitoring (CPU, Memory, Goroutines)

### 🎛️ **Management Controls**
- Pause/Resume consumers
- Adjust worker pool settings
- Flush queues
- Restart/Stop broker operations
- Configuration management

## Quick Start

### 1. Run the Admin Demo

```bash
cd examples/admin
go run main.go
```

This will start:
- MQ Broker (background)
- Admin Dashboard on http://localhost:8090/admin
- Sample task simulation for demonstration

### 2. Access the Dashboard

Open your browser and navigate to:
```
http://localhost:8090/admin
```

### 3. Explore the Interface

The dashboard consists of several tabs:

- **📈 Overview**: High-level metrics and system status
- **🔧 Broker**: Broker configuration and control
- **📋 Queues**: Queue monitoring and management
- **👥 Consumers**: Consumer status and control
- **🏊 Pools**: Worker pool monitoring
- **❤️ Monitoring**: Health checks and system metrics

## Integration in Your Application

### Basic Usage

```go
package main

import (
    "context"
    "log"

    "github.com/oarkflow/mq"
    "github.com/oarkflow/mq/logger"
)

func main() {
    // Create logger
    lg := logger.NewDefaultLogger()

    // Create broker
    broker := mq.NewBroker(mq.WithLogger(lg))

    // Start broker
    ctx := context.Background()
    if err := broker.Start(ctx); err != nil {
        log.Fatalf("Failed to start broker: %v", err)
    }
    defer broker.Close()

    // Create admin server
    adminServer := mq.NewAdminServer(broker, ":8090", lg)
    if err := adminServer.Start(); err != nil {
        log.Fatalf("Failed to start admin server: %v", err)
    }
    defer adminServer.Stop()

    // Your application logic here...
}
```

### Configuration Options

The admin server can be configured with different options:

```go
// Custom port
adminServer := mq.NewAdminServer(broker, ":9090", lg)

// With custom logger
customLogger := logger.NewDefaultLogger()
adminServer := mq.NewAdminServer(broker, ":8090", customLogger)
```

## API Endpoints

The admin server exposes several REST API endpoints:

### Core Endpoints
- `GET /admin` - Main dashboard interface
- `GET /api/admin/metrics` - Real-time metrics
- `GET /api/admin/broker` - Broker information
- `GET /api/admin/queues` - Queue status
- `GET /api/admin/consumers` - Consumer information
- `GET /api/admin/pools` - Worker pool status
- `GET /api/admin/health` - Health checks

### Control Endpoints
- `POST /api/admin/broker/restart` - Restart broker
- `POST /api/admin/broker/stop` - Stop broker
- `POST /api/admin/queues/flush` - Flush all queues

### Example API Usage

```bash
# Get current metrics
curl http://localhost:8090/api/admin/metrics

# Get broker status
curl http://localhost:8090/api/admin/broker

# Flush all queues
curl -X POST http://localhost:8090/api/admin/queues/flush
```

## Dashboard Features

### 🎨 **Modern UI Design**
- Responsive design with Tailwind CSS
- Clean, intuitive interface
- Dark/light theme support
- Mobile-friendly layout

### 📊 **Real-time Charts**
- Live throughput monitoring
- Queue depth trends
- System resource usage
- Error rate tracking

### ⚡ **Interactive Controls**
- Consumer pause/resume buttons
- Pool configuration modals
- Queue management tools
- Real-time status updates

### 🔄 **WebSocket Integration**
- Live data updates without page refresh
- Real-time event streaming
- Automatic reconnection
- Low-latency monitoring

## File Structure

The admin interface consists of:

```
static/admin/
├── index.html          # Main dashboard interface
├── css/
│   └── admin.css      # Custom styling
└── js/
    └── admin.js       # Dashboard JavaScript logic
```

## Metrics and Monitoring

### System Metrics
- **Throughput**: Messages processed per second
- **Queue Depth**: Number of pending messages
- **Active Consumers**: Currently running consumers
- **Error Rate**: Failed message percentage
- **Memory Usage**: System memory consumption
- **CPU Usage**: System CPU utilization

### Queue Metrics
- Message count per queue
- Consumer count per queue
- Processing rate per queue
- Error count per queue

### Consumer Metrics
- Messages processed
- Error count
- Last activity timestamp
- Configuration parameters

### Pool Metrics
- Active workers
- Queue size
- Memory load
- Task distribution

## Customization

### Styling
The interface uses Tailwind CSS and can be customized by modifying `static/admin/css/admin.css`.

### JavaScript
Dashboard functionality can be extended by modifying `static/admin/js/admin.js`.

### Backend
Additional API endpoints can be added to `admin_server.go`.

## Production Considerations

### Security
- Add authentication/authorization
- Use HTTPS in production
- Implement rate limiting
- Add CORS configuration

### Performance
- Enable caching for static assets
- Use compression middleware
- Monitor memory usage
- Implement connection pooling

### Monitoring
- Add logging for admin operations
- Implement audit trails
- Monitor admin API usage
- Set up alerting for critical events

## Troubleshooting

### Common Issues

1. **Dashboard not loading**
   - Check if static files are in the correct location
   - Verify server is running on correct port
   - Check browser console for errors

2. **API endpoints returning 404**
   - Ensure admin server is started
   - Verify correct port configuration
   - Check route registration

3. **Real-time updates not working**
   - Check WebSocket connection in browser dev tools
   - Verify broker is running and accessible
   - Check for CORS issues

### Debug Mode

Enable debug logging:

```go
lg := logger.NewDefaultLogger()
lg.Debug("Admin server starting")
```

## License

This admin dashboard is part of the MQ package and follows the same license terms.

## Contributing

To contribute to the admin dashboard:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

For issues or feature requests, please open an issue on the main repository.
