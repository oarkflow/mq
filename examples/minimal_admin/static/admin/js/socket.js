class Socket {
    events = {}
    reconnectInterval
    reconnectOpts = { enabled: true, replayOnConnect: true, intervalMS: 5000 }
    reconnecting = false
    connectedOnce = false
    headerStartCharCode = 1
    headerStartChar
    dataStartCharCode = 2
    dataStartChar
    subProtocol = 'sac-sock'
    ws
    reconnected = false
    maxAttempts = 3
    totalAttempts = 0
    kickedOut = false
    userID
    url

    constructor(url, userID, opts = { reconnectOpts: {} }) {
        opts = opts || { reconnectOpts: {} };
        this.headerStartChar = String.fromCharCode(this.headerStartCharCode)
        this.dataStartChar = String.fromCharCode(this.dataStartCharCode)
        this.url = url
        this.userID = userID
        if (typeof opts.reconnectOpts == 'object') {
            for (let i in opts.reconnectOpts) {
                if (!opts.reconnectOpts.hasOwnProperty(i)) continue;
                this.reconnectOpts[i] = opts.reconnectOpts[i];
            }
        }
    }

    noop() { }

    async connect(timeout = 10000) {
        try {
            this.ws = new WebSocket(this.url, this.subProtocol);
            this.ws.binaryType = 'arraybuffer';
            this.handleEvents()
            const isOpened = () => (this.ws.readyState === WebSocket.OPEN)

            if (this.ws.readyState !== WebSocket.CONNECTING) {
                return isOpened()
            }
        } catch (err) {
            console.log("Error on reconnection", err)
        }

    }

    handleEvents() {
        let self = this
        this.onConnect(this.noop)
        this.onDisconnect(this.noop)
        this.ws.onmessage = function (e) {
            let msg = e.data,
                headers = {},
                eventName = '',
                data = '',
                chr = null,
                i, msgLen;

            if (typeof msg === 'string') {
                let dataStarted = false,
                    headerStarted = false;

                for (i = 0, msgLen = msg.length; i < msgLen; i++) {
                    chr = msg[i];
                    if (!dataStarted && !headerStarted && chr !== self.dataStartChar && chr !== self.headerStartChar) {
                        eventName += chr;
                    } else if (!headerStarted && chr === self.headerStartChar) {
                        headerStarted = true;
                    } else if (headerStarted && !dataStarted && chr !== self.dataStartChar) {
                        headers[chr] = true;
                    } else if (!dataStarted && chr === self.dataStartChar) {
                        dataStarted = true;
                    } else {
                        data += chr;
                    }
                }
            } else if (msg && msg instanceof ArrayBuffer && msg.byteLength !== undefined) {
                let dv = new DataView(msg),
                    headersStarted = false;

                for (i = 0, msgLen = dv.byteLength; i < msgLen; i++) {
                    chr = dv.getUint8(i);

                    if (chr !== self.dataStartCharCode && chr !== self.headerStartCharCode && !headersStarted) {
                        eventName += String.fromCharCode(chr);
                    } else if (chr === self.headerStartCharCode && !headersStarted) {
                        headersStarted = true;
                    } else if (headersStarted && chr !== self.dataStartCharCode) {
                        headers[String.fromCharCode(chr)] = true;
                    } else if (chr === self.dataStartCharCode) {
                        // @ts-ignore
                        data = dv.buffer.slice(i + 1);
                        break;
                    }
                }
            }

            if (eventName.length === 0) return; //no event to dispatch
            if (typeof self.events[eventName] === 'undefined') return;
            // @ts-ignore
            self.events[eventName].call(self, (headers.J) ? JSON.parse(data) : data);
        }
    }

    startReconnect(timeout = 10000) {
        let self = this
        setTimeout(async function () {
            try {
                if (self.maxAttempts > self.totalAttempts) {
                    return
                }
                let newWS = new WebSocket(self.url, self.subProtocol);
                self.totalAttempts += 1
                console.log("attempt to reconnect...", self.totalAttempts)
                newWS.onmessage = self.ws.onmessage;
                newWS.onclose = self.ws.onclose;
                newWS.binaryType = self.ws.binaryType;
                self.handleEvents()

                //we need to run the initially set onConnect function on the first successful connecting,
                //even if replayOnConnect is disabled. The server might not be available on a first
                //connection attempt.
                if (self.reconnectOpts.replayOnConnect || !self.connectedOnce) {
                    newWS.onopen = self.ws.onopen;
                }
                self.ws = newWS;
                if (!self.reconnectOpts.replayOnConnect && self.connectedOnce) {
                    self.onConnect(self.noop);
                }
                self.ws = newWS
                const isOpened = () => (self.ws.readyState === WebSocket.OPEN)

                if (self.ws.readyState !== WebSocket.CONNECTING) {
                    const opened = isOpened()
                    if (!opened) {
                        self.startReconnect(timeout)
                    } else {
                        console.log("connected with signal server")
                    }
                    return opened
                }
                else {
                    const intrasleep = 100
                    const ttl = timeout / intrasleep // time to loop
                    let loop = 0
                    while (self.ws.readyState === WebSocket.CONNECTING && loop < ttl) {
                        await new Promise(resolve => setTimeout(resolve, intrasleep))
                        loop++
                    }
                    const opened = isOpened()
                    if (!opened) {
                        self.startReconnect(timeout)
                    } else {
                        console.log("connected with signal server")
                    }
                    return opened
                }
            } catch (err) {
                console.log("Error on reconnection", err)
            }
        }, self.reconnectOpts.intervalMS);
    }

    onConnect(callback) {
        let self = this
        this.ws.onopen = function () {
            self.connectedOnce = true;
            callback.apply(self, arguments);
            if (self.reconnecting) {
                self.reconnecting = false;
            }
        };
    };

    onDisconnect(callback) {
        let self = this
        this.ws.onclose = function () {
            if (!self.reconnecting && self.connectedOnce) {
                callback.apply(self, arguments);
            }
            if (self.reconnectOpts.enabled && !self.kickedOut) {
                self.reconnecting = true;
                self.startReconnect();
            }
        };
    };

    on(eventName, callback, override) {
        override = override || false
        if (!this.events.hasOwnProperty(eventName)) {
            this.events[eventName] = callback;
        } else if (override) {
            this.off(eventName)
            this.events[eventName] = callback;
        }
    }
    off(eventName) {
        if (this.events[eventName]) {
            delete this.events[eventName];
        }
    }

    emit(eventName, data) {
        let rs = this.ws.readyState;
        if (rs === 0) {
            console.warn("websocket is not open yet");
            return;
        } else if (rs === 2 || rs === 3) {
            console.error("websocket is closed");
            return;
        }
        let msg;
        if (data instanceof ArrayBuffer) {
            let ab = new ArrayBuffer(data.byteLength + eventName.length + 1),
                newBuf = new DataView(ab),
                oldBuf = new DataView(data),
                i = 0;
            for (let evtLen = eventName.length; i < evtLen; i++) {
                newBuf.setUint8(i, eventName.charCodeAt(i));
            }
            newBuf.setUint8(i, this.dataStartCharCode);
            i++;
            for (let x = 0, xLen = oldBuf.byteLength; x < xLen; x++, i++) {
                newBuf.setUint8(i, oldBuf.getUint8(x));
            }
            msg = ab;
        } else if (typeof data === 'object') {
            msg = eventName + this.dataStartChar + JSON.stringify(data);
        } else {
            msg = eventName + this.dataStartChar + data;
        }
        this.ws.send(msg);
    }

    close() {
        this.reconnectOpts.enabled = false;
        return this.ws.close(1000);
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.Socket = Socket;
});
