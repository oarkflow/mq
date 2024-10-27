(function(SS) {
    'use strict';
    let uiContent
    function loadSVG(url) {
        fetch(url)
            .then(response => response.text())
            .then(svgContent => {
                const container = document.getElementById('svg-container');
                container.src = url;
                uiContent = svgContent;
            })
            .catch(err => console.error('Failed to load SVG:', err));
    }

    window.onload = function() {
        loadSVG('http://localhost:8083/ui');
    };
    document.getElementById('send-request').addEventListener('click', function() {
        const input = document.getElementById('payload');
        const payloadData = JSON.parse(input.value);
        const data = { payload: payloadData };

        fetch('http://localhost:8083/request', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data),
        })
            .then(response => response.json())
            .then(data => console.log('Success:', data))
            .catch((error) => console.error('Error:', error));
    });

    const tasks = {};

    function attachSVGNodeEvents() {
        const svgNodes = document.querySelectorAll('g.node');  // Adjust selector as per your SVG structure
        svgNodes.forEach(node => {
            node.classList.add("cursor-pointer")
            node.addEventListener('click', handleSVGNodeClick);
        });
    }

    // Function to handle the click on an SVG node and show the popover
    function handleSVGNodeClick(event) {
        const nodeId = event.currentTarget.id;  // Get the node ID (e.g., 'node_store:data')
        const nodeData = findNodeDataById(nodeId);  // Fetch data related to the node (status, result, etc.)
        if (nodeData) {
            showSVGPopover(event, nodeData);
        }
    }

    // Function to show the popover next to the clicked SVG node
    function showSVGPopover(event, nodeData) {
        const popover = document.getElementById('svg-popover');
        document.getElementById('task-id').innerHTML = nodeData.task_id
        popover.classList.add('visible');
        popover.innerHTML = `
                <div class="text-sm text-gray-600 space-y-1">
                    <div class="grid grid-cols-5 gap-x-4">
                        <strong class="text-gray-800 col-span-1">Node:</strong>
                        <span class="text-gray-700 flex-grow break-words col-span-4">${nodeData.topic}</span>

                        <strong class="text-gray-800 col-span-1">Status:</strong>
                        <span class="text-gray-700 flex-grow break-words col-span-4">${nodeData.status}</span>

                        <strong class="text-gray-800 col-span-1">Result:</strong>
                        <pre class="text-gray-700 col-span-4 p-2 border border-gray-300 rounded-md max-h-32 overflow-auto bg-gray-50">${JSON.stringify(nodeData.payload, null, 2)}
                        </pre>

                        <strong class="text-gray-800 col-span-1">Error:</strong>
                        <span class="text-gray-700 flex-grow break-words col-span-4">${nodeData.error || 'N/A'}</span>

                        <strong class="text-gray-800 col-span-1">Created At:</strong>
                        <span class="text-gray-700 flex-grow break-words col-span-4">${nodeData.created_at}</span>

                        <strong class="text-gray-800 col-span-1">Processed At:</strong>
                        <span class="text-gray-700 flex-grow break-words col-span-4">${nodeData.processed_at}</span>

                        <strong class="text-gray-800 col-span-1">Latency:</strong>
                        <span class="text-gray-700 flex-grow break-words col-span-4">${nodeData.latency}</span>
                    </div>
                </div>

            `;
    }

    // Function to find node data (status, result, error) by the node's ID
    function findNodeDataById(nodeId) {
        for (const taskId in tasks) {
            const task = tasks[taskId];
            const node = task.nodes.find(n => `node_${n.topic}` === nodeId);  // Ensure the ID format matches your SVG
            if (node) {
                return node;
            }
        }
        return null;  // Return null if no matching node is found
    }

    function addOrUpdateTask(message, isFinal = false) {
        const taskTableBody = document.getElementById('taskTableBody');
        const taskId = message.task_id;
        const rowId = `row-${taskId}`;
        let existingRow = document.getElementById(rowId);

        if (!existingRow) {
            const row = document.createElement('tr');
            row.id = rowId;
            taskTableBody.insertBefore(row, taskTableBody.firstChild);
            existingRow = row;
        }

        tasks[taskId] = tasks[taskId] || { nodes: [], final: null };
        if (isFinal) tasks[taskId].final = message;
        else tasks[taskId].nodes.push(message);

        const latestStatus = isFinal ? message.status : message.status;
        const statusColor = latestStatus === 'success' ? 'bg-green-100 text-green-700' :
            latestStatus === 'fail' ? 'bg-red-100 text-red-700' : 'bg-yellow-100 text-yellow-700';

        existingRow.innerHTML = `
                <td class="px-4 py-2 border border-gray-300">${taskId}</td>
                <td class="px-4 py-2 border border-gray-300">${new Date(message.created_at).toLocaleString()}</td>
                <td class="px-4 py-2 border border-gray-300">${new Date(message.processed_at).toLocaleString()}</td>
                <td class="px-4 py-2 border border-gray-300">${message.latency}</td>
                <td class="px-4 py-2 border border-gray-300 ${statusColor}">${latestStatus}</td>
                <td class="px-4 py-2 border border-gray-300">
                    <button class="view-btn text-blue-600 hover:underline" data-task-id='${taskId}'>View</button>
                </td>
            `;

        attachViewButtonEvent();
    }

    function attachViewButtonEvent() {
        const buttons = document.querySelectorAll('.view-btn');
        buttons.forEach(button => {
            button.removeEventListener('click', handleViewButtonClick);
            button.addEventListener('click', handleViewButtonClick);
        });
    }

    function handleViewButtonClick(event) {
        document.getElementById("task-svg").innerHTML = uiContent
        attachSVGNodeEvents();
        const taskId = event.target.getAttribute('data-task-id');
        const task = tasks[taskId];
        updateSVGNodes(task);
    }

    function updateSVGNodes(task) {
        console.log(task)
        task.nodes.forEach((node) => {
            const svgNode = document.querySelector(`#node_${node.topic.replace(':', '\\:')}`);
            console.log(svgNode)
            if (svgNode) {
                const fillColor = node.status === 'success' ? '#A5D6A7' : node.status === 'fail' ? '#EF9A9A' : '#FFE082';
                const path = svgNode.querySelector('path');
                if (path) path.setAttribute('fill', fillColor);
            }
        });
    }


    let ss = new SS('ws://' + window.location.host + '/notify');
    ss.onConnect(() => ss.emit('join', "global"));
    ss.onDisconnect(() => alert('chat disconnected'));
    ss.on('message', msg => addOrUpdateTask(msg, false));
    ss.on('final-message', msg => addOrUpdateTask(msg, true));
})(window.SS);