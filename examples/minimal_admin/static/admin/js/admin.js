// MQ Admin Dashboard JavaScript
class MQAdminDashboard {
    constructor() {
        this.wsConnection = null;
        this.isConnected = false;
        this.charts = {};
        this.refreshInterval = null;
        this.currentTab = 'overview';
        this.data = {
            metrics: {},
            queues: [],
            consumers: [],
            pools: [],
            broker: {},
            healthChecks: []
        };

        this.init();
    }

    init() {
        this.setupEventListeners();
        this.initializeCharts();
        this.connectWebSocket();
        this.startRefreshInterval();
        this.loadInitialData();
    }

    setupEventListeners() {
        // Tab navigation
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                this.switchTab(e.target.dataset.tab);
            });
        });

        // Refresh button
        document.getElementById('refreshBtn').addEventListener('click', () => {
            this.refreshData();
        });

        // Modal handlers
        this.setupModalHandlers();

        // Broker controls
        this.setupBrokerControls();

        // Form handlers
        this.setupFormHandlers();
    }

    setupModalHandlers() {
        // Consumer modal
        const consumerModal = document.getElementById('consumerModal');
        const cancelConsumerBtn = document.getElementById('cancelConsumerConfig');

        cancelConsumerBtn.addEventListener('click', () => {
            consumerModal.classList.add('hidden');
        });

        // Pool modal
        const poolModal = document.getElementById('poolModal');
        const cancelPoolBtn = document.getElementById('cancelPoolConfig');

        cancelPoolBtn.addEventListener('click', () => {
            poolModal.classList.add('hidden');
        });

        // Close modals on backdrop click
        [consumerModal, poolModal].forEach(modal => {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    modal.classList.add('hidden');
                }
            });
        });
    }

    setupBrokerControls() {
        document.getElementById('restartBroker').addEventListener('click', () => {
            this.confirmAction('restart broker', () => this.restartBroker());
        });

        document.getElementById('stopBroker').addEventListener('click', () => {
            this.confirmAction('stop broker', () => this.stopBroker());
        });

        document.getElementById('flushQueues').addEventListener('click', () => {
            this.confirmAction('flush all queues', () => this.flushQueues());
        });

        // Tasks refresh button
        document.getElementById('refreshTasks').addEventListener('click', () => {
            this.fetchTasks();
        });
    }

    setupFormHandlers() {
        // Consumer form
        document.getElementById('consumerForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.updateConsumerConfig();
        });

        // Pool form
        document.getElementById('poolForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.updatePoolConfig();
        });
    }

    switchTab(tabName) {
        // Update active tab button
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');

        // Show corresponding content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.remove('active');
            content.classList.add('hidden');
        });
        document.getElementById(tabName).classList.remove('hidden');
        document.getElementById(tabName).classList.add('active');

        this.currentTab = tabName;
        this.loadTabData(tabName);
    }

    loadTabData(tabName) {
        switch (tabName) {
            case 'overview':
                this.loadOverviewData();
                break;
            case 'broker':
                this.loadBrokerData();
                break;
            case 'queues':
                this.loadQueuesData();
                break;
            case 'consumers':
                this.loadConsumersData();
                break;
            case 'pools':
                this.loadPoolsData();
                break;
            case 'tasks':
                this.loadTasksData();
                break;
            case 'monitoring':
                this.loadMonitoringData();
                break;
        }
    }

    connectWebSocket() {
        try {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;

            // Use the Socket class instead of raw WebSocket
            this.wsConnection = new Socket(wsUrl, 'admin-user');
            this.wsConnection.connect().then(connected => {
                console.log('WebSocket connected');
                this.wsConnection.onConnect(() => {
                    this.updateConnectionStatus(true);
                    this.showToast('Connected to MQ Admin', 'success');
                    console.log('WebSocket connected');
                });

                this.wsConnection.on('update', (data) => {
                    try {
                        // Data is already parsed by the Socket class
                        this.handleWebSocketMessage(data);
                    } catch (error) {
                        console.error('Failed to handle WebSocket message:', error);
                    }
                });

                this.wsConnection.onDisconnect(() => {
                    this.updateConnectionStatus(false);
                    this.showToast('Disconnected from MQ Admin', 'warning');
                    console.log('WebSocket disconnected');
                    // Socket class handles reconnection automatically
                });
            });
        } catch (error) {
            console.error('Failed to connect WebSocket:', error);
            this.updateConnectionStatus(false);
        }
    }

    handleWebSocketMessage(data) {
        console.log('WebSocket message received:', data);

        switch (data.type) {
            case 'metrics':
                this.updateMetrics(data.data);
                break;
            case 'queues':
                this.updateQueues(data.data);
                break;
            case 'consumers':
                this.updateConsumers(data.data);
                break;
            case 'pools':
                this.updatePools(data.data);
                break;
            case 'broker':
                this.updateBroker(data.data);
                break;
            case 'task_update':
                this.handleTaskUpdate(data.data);
                break;
            case 'broker_restart':
            case 'broker_stop':
            case 'broker_pause':
            case 'broker_resume':
                this.showToast(data.data.message || 'Broker operation completed', 'info');
                // Refresh broker info
                this.fetchBrokerInfo();
                break;
            case 'consumer_pause':
            case 'consumer_resume':
            case 'consumer_stop':
                this.showToast(data.data.message || 'Consumer operation completed', 'info');
                // Refresh consumer info
                this.fetchConsumers();
                break;
            case 'pool_pause':
            case 'pool_resume':
            case 'pool_stop':
                this.showToast(data.data.message || 'Pool operation completed', 'info');
                // Refresh pool info
                this.fetchPools();
                break;
            case 'queue_purge':
            case 'queues_flush':
                this.showToast(data.data.message || 'Queue operation completed', 'info');
                // Refresh queue info
                this.fetchQueues();
                break;
            default:
                console.log('Unknown WebSocket message type:', data.type);
        }
    }

    handleTaskUpdate(taskData) {
        // Add to activity feed
        const activity = {
            type: 'task',
            message: `Task ${taskData.task_id} ${taskData.status} in queue ${taskData.queue}`,
            timestamp: new Date(taskData.updated_at),
            status: taskData.status === 'completed' ? 'success' : taskData.status === 'failed' ? 'error' : 'info'
        };
        this.addActivity(activity);

        // Update metrics if on overview tab
        if (this.currentTab === 'overview') {
            this.fetchMetrics();
        }
    }

    updateConnectionStatus(connected) {
        this.isConnected = connected;
        const indicator = document.getElementById('connectionIndicator');
        const status = document.getElementById('connectionStatus');

        if (connected) {
            indicator.className = 'w-3 h-3 bg-green-500 rounded-full';
            status.textContent = 'Connected';
        } else {
            indicator.className = 'w-3 h-3 bg-red-500 rounded-full';
            status.textContent = 'Disconnected';
        }
    }

    initializeCharts() {
        // Throughput Chart
        const throughputCtx = document.getElementById('throughputChart').getContext('2d');
        this.charts.throughput = new Chart(throughputCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Messages/sec',
                    data: [],
                    borderColor: 'rgb(59, 130, 246)',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false, // Disable animations to prevent loops
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });

        // Queue Depth Chart
        const queueDepthCtx = document.getElementById('queueDepthChart').getContext('2d');
        this.charts.queueDepth = new Chart(queueDepthCtx, {
            type: 'bar',
            data: {
                labels: [],
                datasets: [{
                    label: 'Queue Depth',
                    data: [],
                    backgroundColor: 'rgba(16, 185, 129, 0.8)',
                    borderColor: 'rgb(16, 185, 129)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });

        // System Performance Chart
        const systemCtx = document.getElementById('systemChart').getContext('2d');
        this.charts.system = new Chart(systemCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'CPU %',
                        data: [],
                        borderColor: 'rgb(239, 68, 68)',
                        backgroundColor: 'rgba(239, 68, 68, 0.1)',
                        tension: 0.4
                    },
                    {
                        label: 'Memory %',
                        data: [],
                        borderColor: 'rgb(245, 158, 11)',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        tension: 0.4
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 100
                    }
                }
            }
        });

        // Error Rate Chart
        const errorCtx = document.getElementById('errorChart').getContext('2d');
        this.charts.error = new Chart(errorCtx, {
            type: 'doughnut',
            data: {
                labels: ['Success', 'Failed'],
                datasets: [{
                    data: [0, 0],
                    backgroundColor: [
                        'rgba(16, 185, 129, 0.8)',
                        'rgba(239, 68, 68, 0.8)'
                    ],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }

    async loadInitialData() {
        try {
            await Promise.all([
                this.fetchMetrics(),
                this.fetchQueues(),
                this.fetchConsumers(),
                this.fetchPools(),
                this.fetchBrokerInfo(),
                this.fetchHealthChecks()
            ]);
        } catch (error) {
            console.error('Failed to load initial data:', error);
            this.showToast('Failed to load initial data', 'error');
        }
    }

    async fetchMetrics() {
        try {
            const response = await fetch('/api/admin/metrics');
            const metrics = await response.json();
            this.updateMetrics(metrics);
        } catch (error) {
            console.error('Failed to fetch metrics:', error);
        }
    }

    async fetchQueues() {
        try {
            const response = await fetch('/api/admin/queues');
            const queues = await response.json();
            this.updateQueues(queues);
        } catch (error) {
            console.error('Failed to fetch queues:', error);
        }
    }

    async fetchConsumers() {
        try {
            const response = await fetch('/api/admin/consumers');
            const consumers = await response.json();
            this.updateConsumers(consumers);
        } catch (error) {
            console.error('Failed to fetch consumers:', error);
        }
    }

    async fetchPools() {
        try {
            const response = await fetch('/api/admin/pools');
            const pools = await response.json();
            this.updatePools(pools);
        } catch (error) {
            console.error('Failed to fetch pools:', error);
        }
    }

    async fetchBrokerInfo() {
        try {
            const response = await fetch('/api/admin/broker');
            const broker = await response.json();
            this.updateBroker(broker);
        } catch (error) {
            console.error('Failed to fetch broker info:', error);
        }
    }

    async fetchHealthChecks() {
        try {
            const response = await fetch('/api/admin/health');
            const healthChecks = await response.json();
            this.updateHealthChecks(healthChecks);
        } catch (error) {
            console.error('Failed to fetch health checks:', error);
        }
    }

    async fetchTasks() {
        try {
            const response = await fetch('/api/admin/tasks');
            const data = await response.json();
            this.updateTasks(data.tasks || []);
        } catch (error) {
            console.error('Failed to fetch tasks:', error);
        }
    }

    updateTasks(tasks) {
        this.data.tasks = tasks;

        // Update task count cards
        const activeTasks = tasks.filter(task => task.status === 'queued' || task.status === 'processing').length;
        const completedTasks = tasks.filter(task => task.status === 'completed').length;
        const failedTasks = tasks.filter(task => task.status === 'failed').length;
        const queuedTasks = tasks.filter(task => task.status === 'queued').length;

        document.getElementById('activeTasks').textContent = activeTasks;
        document.getElementById('completedTasks').textContent = completedTasks;
        document.getElementById('failedTasks').textContent = failedTasks;
        document.getElementById('queuedTasks').textContent = queuedTasks;

        // Update tasks table
        this.renderTasksTable(tasks);
    }

    renderTasksTable(tasks) {
        const tbody = document.getElementById('tasksTableBody');
        tbody.innerHTML = '';

        if (!tasks || tasks.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="px-6 py-4 text-sm text-gray-500 text-center">
                        No tasks found
                    </td>
                </tr>
            `;
            return;
        }

        tasks.forEach(task => {
            const row = document.createElement('tr');
            row.className = 'hover:bg-gray-50';

            const statusClass = this.getStatusClass(task.status);
            const createdAt = this.formatTime(task.created_at);
            const payload = typeof task.payload === 'string' ? task.payload : JSON.stringify(task.payload);
            const truncatedPayload = payload.length > 50 ? payload.substring(0, 50) + '...' : payload;

            row.innerHTML = `
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    ${task.id}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    ${task.queue}
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusClass}">
                        ${task.status}
                    </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    ${task.retry_count || 0}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    ${createdAt}
                </td>
                <td class="px-6 py-4 text-sm text-gray-500" title="${payload}">
                    ${truncatedPayload}
                </td>
            `;

            tbody.appendChild(row);
        });
    }

    updateMetrics(metrics) {
        this.data.metrics = metrics;

        // Update overview cards
        document.getElementById('totalMessages').textContent = this.formatNumber(metrics.total_messages || 0);
        document.getElementById('activeConsumers').textContent = metrics.active_consumers || 0;
        document.getElementById('activeQueues').textContent = metrics.active_queues || 0;
        document.getElementById('failedMessages').textContent = this.formatNumber(metrics.failed_messages || 0);

        // Update charts
        this.updateThroughputChart(metrics.throughput_history || []);
        this.updateErrorChart(metrics.success_count || 0, metrics.error_count || 0);
    }

    updateQueues(queues) {
        this.data.queues = queues;
        this.renderQueuesTable(queues);
        this.updateQueueDepthChart(queues);
    }

    updateConsumers(consumers) {
        this.data.consumers = consumers;
        this.renderConsumersTable(consumers);
    }

    updatePools(pools) {
        this.data.pools = pools;
        this.renderPoolsTable(pools);
    }

    updateBroker(broker) {
        this.data.broker = broker;
        this.renderBrokerInfo(broker);
    }

    updateHealthChecks(healthChecks) {
        this.data.healthChecks = healthChecks;
        this.renderHealthChecks(healthChecks);
    }

    updateThroughputChart(throughputHistory) {
        const chart = this.charts.throughput;
        if (!chart || !throughputHistory) return;

        try {
            const now = new Date();

            // Keep last 20 data points
            chart.data.labels = throughputHistory.map((_, index) => {
                const time = new Date(now.getTime() - (throughputHistory.length - index - 1) * 5000);
                return time.toLocaleTimeString();
            });

            chart.data.datasets[0].data = throughputHistory;
            chart.update('none');
        } catch (error) {
            console.error('Error updating throughput chart:', error);
        }
    }

    updateQueueDepthChart(queues) {
        const chart = this.charts.queueDepth;
        if (!chart || !queues) return;

        try {
            chart.data.labels = queues.map(q => q.name);
            chart.data.datasets[0].data = queues.map(q => q.depth || 0);
            chart.update('none');
        } catch (error) {
            console.error('Error updating queue depth chart:', error);
        }
    }

    updateErrorChart(successCount, errorCount) {
        const chart = this.charts.error;
        if (!chart) return;

        try {
            chart.data.datasets[0].data = [successCount, errorCount];
            chart.update('none');
        } catch (error) {
            console.error('Error updating error chart:', error);
        }
    }

    renderQueuesTable(queues) {
        const tbody = document.getElementById('queuesTable');
        tbody.innerHTML = '';

        queues.forEach(queue => {
            const row = document.createElement('tr');
            row.className = 'table-row';
            row.innerHTML = `
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${queue.name}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${queue.depth || 0}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${queue.consumers || 0}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${queue.rate || 0}/sec</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <button onclick="adminDashboard.purgeQueue('${queue.name}')" class="text-red-600 hover:text-red-900 mr-2">
                        Purge
                    </button>
                    <button onclick="adminDashboard.configureQueue('${queue.name}')" class="text-blue-600 hover:text-blue-900">
                        Configure
                    </button>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    renderConsumersTable(consumers) {
        const tbody = document.getElementById('consumersTable');
        tbody.innerHTML = '';

        consumers.forEach(consumer => {
            const row = document.createElement('tr');
            row.className = 'table-row';
            row.innerHTML = `
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${consumer.id}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${consumer.queue}</td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="status-indicator ${this.getStatusClass(consumer.status)}">${consumer.status}</span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${consumer.processed || 0}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${consumer.errors || 0}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <button onclick="adminDashboard.pauseConsumer('${consumer.id}')" class="text-yellow-600 hover:text-yellow-900 mr-2">
                        ${consumer.status === 'paused' ? 'Resume' : 'Pause'}
                    </button>
                    <button onclick="adminDashboard.configureConsumer('${consumer.id}')" class="text-blue-600 hover:text-blue-900 mr-2">
                        Configure
                    </button>
                    <button onclick="adminDashboard.stopConsumer('${consumer.id}')" class="text-red-600 hover:text-red-900">
                        Stop
                    </button>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    renderPoolsTable(pools) {
        const tbody = document.getElementById('poolsTable');
        tbody.innerHTML = '';

        pools.forEach(pool => {
            const row = document.createElement('tr');
            row.className = 'table-row';
            row.innerHTML = `
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">${pool.id}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${pool.workers || 0}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${pool.queue_size || 0}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">${pool.active_tasks || 0}</td>
                <td class="px-6 py-4 whitespace-nowrap">
                    <span class="status-indicator ${this.getStatusClass(pool.status)}">${pool.status}</span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <button onclick="adminDashboard.pausePool('${pool.id}')" class="text-yellow-600 hover:text-yellow-900 mr-2">
                        ${pool.status === 'paused' ? 'Resume' : 'Pause'}
                    </button>
                    <button onclick="adminDashboard.configurePool('${pool.id}')" class="text-blue-600 hover:text-blue-900 mr-2">
                        Configure
                    </button>
                    <button onclick="adminDashboard.stopPool('${pool.id}')" class="text-red-600 hover:text-red-900">
                        Stop
                    </button>
                </td>
            `;
            tbody.appendChild(row);
        });
    }

    renderBrokerInfo(broker) {
        document.getElementById('brokerStatus').textContent = broker.status || 'Unknown';
        document.getElementById('brokerStatus').className = `px-2 py-1 text-xs rounded-full ${this.getStatusClass(broker.status)}`;
        document.getElementById('brokerAddress').textContent = broker.address || 'N/A';
        document.getElementById('brokerUptime').textContent = this.formatDuration(broker.uptime || 0);
        document.getElementById('brokerConnections').textContent = broker.connections || 0;

        // Render broker configuration
        this.renderBrokerConfig(broker.config || {});
    }

    renderBrokerConfig(config) {
        const container = document.getElementById('brokerConfig');
        container.innerHTML = '';

        Object.entries(config).forEach(([key, value]) => {
            const div = document.createElement('div');
            div.className = 'flex justify-between items-center p-3 bg-gray-50 rounded';
            div.innerHTML = `
                <span class="font-medium text-gray-700">${this.formatConfigKey(key)}:</span>
                <span class="text-gray-600">${this.formatConfigValue(value)}</span>
            `;
            container.appendChild(div);
        });
    }

    renderHealthChecks(healthChecks) {
        const container = document.getElementById('healthChecks');
        container.innerHTML = '';

        healthChecks.forEach(check => {
            const div = document.createElement('div');
            div.className = `health-check ${check.status}`;
            div.innerHTML = `
                <div class="flex items-center justify-between w-full">
                    <div class="flex items-center">
                        <div class="health-check-icon">
                            ${this.getHealthIcon(check.status)}
                        </div>
                        <div>
                            <div class="font-medium">${check.name}</div>
                            <div class="text-sm opacity-75">${check.message}</div>
                        </div>
                    </div>
                    <div class="text-sm">
                        ${this.formatDuration(check.duration)}
                    </div>
                </div>
            `;
            container.appendChild(div);
        });
    }

    addActivity(activity) {
        const feed = document.getElementById('activityFeed');
        const item = document.createElement('li');
        item.className = 'activity-item';

        item.innerHTML = `
            <div class="activity-icon ${activity.type}">
                ${this.getActivityIcon(activity.type)}
            </div>
            <div class="flex-1">
                <div class="text-sm font-medium text-gray-900">${activity.title}</div>
                <div class="text-sm text-gray-500">${activity.description}</div>
                <div class="text-xs text-gray-400">${this.formatTime(activity.timestamp)}</div>
            </div>
        `;

        feed.insertBefore(item, feed.firstChild);

        // Keep only last 50 items
        while (feed.children.length > 50) {
            feed.removeChild(feed.lastChild);
        }
    }

    // Action methods
    async pauseConsumer(consumerId) {
        try {
            const consumer = this.data.consumers.find(c => c.id === consumerId);
            const action = consumer?.status === 'paused' ? 'resume' : 'pause';

            const response = await fetch(`/api/admin/consumers/${action}?id=${consumerId}`, {
                method: 'POST'
            });

            if (response.ok) {
                this.showToast(`Consumer ${action}d successfully`, 'success');
                this.fetchConsumers();
            } else {
                throw new Error(`Failed to ${action} consumer`);
            }
        } catch (error) {
            this.showToast(error.message, 'error');
        }
    }

    async stopConsumer(consumerId) {
        this.confirmAction('stop this consumer', async () => {
            try {
                const response = await fetch(`/api/admin/consumers/stop?id=${consumerId}`, {
                    method: 'POST'
                });

                if (response.ok) {
                    this.showToast('Consumer stopped successfully', 'success');
                    this.fetchConsumers();
                } else {
                    throw new Error('Failed to stop consumer');
                }
            } catch (error) {
                this.showToast(error.message, 'error');
            }
        });
    }

    configureConsumer(consumerId) {
        const consumer = this.data.consumers.find(c => c.id === consumerId);
        if (!consumer) return;

        document.getElementById('consumerIdField').value = consumer.id;
        document.getElementById('maxConcurrentTasks').value = consumer.max_concurrent_tasks || 10;
        document.getElementById('taskTimeout').value = consumer.task_timeout || 30;
        document.getElementById('maxRetries').value = consumer.max_retries || 3;

        document.getElementById('consumerModal').classList.remove('hidden');
    }

    async updateConsumerConfig() {
        try {
            const consumerId = document.getElementById('consumerIdField').value;
            const config = {
                max_concurrent_tasks: parseInt(document.getElementById('maxConcurrentTasks').value),
                task_timeout: parseInt(document.getElementById('taskTimeout').value),
                max_retries: parseInt(document.getElementById('maxRetries').value)
            };

            const response = await fetch(`/api/admin/consumers/${consumerId}/config`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(config)
            });

            if (response.ok) {
                this.showToast('Consumer configuration updated', 'success');
                document.getElementById('consumerModal').classList.add('hidden');
                this.fetchConsumers();
            } else {
                throw new Error('Failed to update consumer configuration');
            }
        } catch (error) {
            this.showToast(error.message, 'error');
        }
    }

    async pausePool(poolId) {
        try {
            const pool = this.data.pools.find(p => p.id === poolId);
            const action = pool?.status === 'paused' ? 'resume' : 'pause';

            const response = await fetch(`/api/admin/pools/${poolId}/${action}`, {
                method: 'POST'
            });

            if (response.ok) {
                this.showToast(`Pool ${action}d successfully`, 'success');
                this.fetchPools();
            } else {
                throw new Error(`Failed to ${action} pool`);
            }
        } catch (error) {
            this.showToast(error.message, 'error');
        }
    }

    async stopPool(poolId) {
        this.confirmAction('stop this pool', async () => {
            try {
                const response = await fetch(`/api/admin/pools/${poolId}/stop`, {
                    method: 'POST'
                });

                if (response.ok) {
                    this.showToast('Pool stopped successfully', 'success');
                    this.fetchPools();
                } else {
                    throw new Error('Failed to stop pool');
                }
            } catch (error) {
                this.showToast(error.message, 'error');
            }
        });
    }

    configurePool(poolId) {
        const pool = this.data.pools.find(p => p.id === poolId);
        if (!pool) return;

        document.getElementById('poolIdField').value = pool.id;
        document.getElementById('numWorkers').value = pool.workers || 4;
        document.getElementById('queueSize').value = pool.queue_size || 100;
        document.getElementById('maxMemoryLoad').value = pool.max_memory_load || 5000000;

        document.getElementById('poolModal').classList.remove('hidden');
    }

    async updatePoolConfig() {
        try {
            const poolId = document.getElementById('poolIdField').value;
            const config = {
                workers: parseInt(document.getElementById('numWorkers').value),
                queue_size: parseInt(document.getElementById('queueSize').value),
                max_memory_load: parseInt(document.getElementById('maxMemoryLoad').value)
            };

            const response = await fetch(`/api/admin/pools/${poolId}/config`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(config)
            });

            if (response.ok) {
                this.showToast('Pool configuration updated', 'success');
                document.getElementById('poolModal').classList.add('hidden');
                this.fetchPools();
            } else {
                throw new Error('Failed to update pool configuration');
            }
        } catch (error) {
            this.showToast(error.message, 'error');
        }
    }

    async purgeQueue(queueName) {
        this.confirmAction(`purge queue "${queueName}"`, async () => {
            try {
                const response = await fetch(`/api/admin/queues/${queueName}/purge`, {
                    method: 'POST'
                });

                if (response.ok) {
                    this.showToast('Queue purged successfully', 'success');
                    this.fetchQueues();
                } else {
                    throw new Error('Failed to purge queue');
                }
            } catch (error) {
                this.showToast(error.message, 'error');
            }
        });
    }

    async restartBroker() {
        try {
            const response = await fetch('/api/admin/broker/restart', {
                method: 'POST'
            });

            if (response.ok) {
                this.showToast('Broker restart initiated', 'success');
                this.fetchBrokerInfo();
            } else {
                throw new Error('Failed to restart broker');
            }
        } catch (error) {
            this.showToast(error.message, 'error');
        }
    }

    async stopBroker() {
        try {
            const response = await fetch('/api/admin/broker/stop', {
                method: 'POST'
            });

            if (response.ok) {
                this.showToast('Broker stop initiated', 'warning');
                this.fetchBrokerInfo();
            } else {
                throw new Error('Failed to stop broker');
            }
        } catch (error) {
            this.showToast(error.message, 'error');
        }
    }

    async flushQueues() {
        try {
            const response = await fetch('/api/admin/queues/flush', {
                method: 'POST'
            });

            if (response.ok) {
                this.showToast('All queues flushed', 'success');
                this.fetchQueues();
            } else {
                throw new Error('Failed to flush queues');
            }
        } catch (error) {
            this.showToast(error.message, 'error');
        }
    }

    // Utility methods
    confirmAction(action, callback) {
        if (confirm(`Are you sure you want to ${action}?`)) {
            callback();
        }
    }

    showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `
            <div class="flex items-center justify-between">
                <span>${message}</span>
                <button onclick="this.parentElement.parentElement.remove()" class="ml-4 text-gray-500 hover:text-gray-700">
                    ×
                </button>
            </div>
        `;

        document.body.appendChild(toast);

        // Show toast
        setTimeout(() => toast.classList.add('show'), 100);

        // Auto-remove after 5 seconds
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, 5000);
    }

    formatNumber(num) {
        if (num >= 1000000) {
            return (num / 1000000).toFixed(1) + 'M';
        } else if (num >= 1000) {
            return (num / 1000).toFixed(1) + 'K';
        }
        return num.toString();
    }

    formatDuration(ms) {
        if (ms < 1000) return `${ms}ms`;
        if (ms < 60000) return `${Math.floor(ms / 1000)}s`;
        if (ms < 3600000) return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
        const hours = Math.floor(ms / 3600000);
        const minutes = Math.floor((ms % 3600000) / 60000);
        return `${hours}h ${minutes}m`;
    }

    formatTime(timestamp) {
        return new Date(timestamp).toLocaleTimeString();
    }

    formatConfigKey(key) {
        return key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }

    formatConfigValue(value) {
        if (typeof value === 'boolean') return value ? 'Yes' : 'No';
        if (typeof value === 'object') return JSON.stringify(value);
        return value.toString();
    }

    getStatusClass(status) {
        switch (status?.toLowerCase()) {
            case 'running':
            case 'active':
            case 'healthy':
                return 'status-running';
            case 'paused':
            case 'warning':
                return 'status-paused';
            case 'stopped':
            case 'error':
            case 'failed':
                return 'status-stopped';
            default:
                return 'status-paused';
        }
    }

    getHealthIcon(status) {
        switch (status) {
            case 'healthy':
                return '✓';
            case 'warning':
                return '⚠';
            case 'error':
                return '✗';
            default:
                return '?';
        }
    }

    getActivityIcon(type) {
        switch (type) {
            case 'success':
                return '✓';
            case 'warning':
                return '⚠';
            case 'error':
                return '✗';
            case 'info':
            default:
                return 'i';
        }
    }

    refreshData() {
        this.loadInitialData();
        this.showToast('Data refreshed', 'success');
    }

    startRefreshInterval() {
        // Clear any existing interval first
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }

        // Refresh data every 15 seconds (reduced frequency to prevent overload)
        this.refreshInterval = setInterval(() => {
            if (this.isConnected && this.currentTab) {
                try {
                    this.loadTabData(this.currentTab);
                } catch (error) {
                    console.error('Error during refresh:', error);
                }
            }
        }, 15000);
    }

    loadOverviewData() {
        this.fetchMetrics();
    }

    loadBrokerData() {
        this.fetchBrokerInfo();
    }

    loadQueuesData() {
        this.fetchQueues();
    }

    loadConsumersData() {
        this.fetchConsumers();
    }

    loadPoolsData() {
        this.fetchPools();
    }

    loadTasksData() {
        this.fetchTasks();
    }

    loadMonitoringData() {
        this.fetchHealthChecks();
        this.fetchMetrics();
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.adminDashboard = new MQAdminDashboard();
});
