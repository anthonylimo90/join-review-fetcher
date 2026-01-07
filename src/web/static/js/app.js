/**
 * Safari Review Scraper - Alpine.js Application
 */
function scraperApp() {
    return {
        // WebSocket
        ws: null,
        wsConnected: false,
        reconnectInterval: null,

        // Config
        config: {
            source: 'safaribookings',
            maxOperators: 50,
            maxReviews: 50,
            headless: true,
            resume: true,
        },

        // Status
        isRunning: false,
        sleepPrevented: false,
        status: {
            current_operator_index: 0,
            total_operators: 0,
            total_reviews: 0,
            current_operator: '',
            current_page: 1,
            reviews_on_current_operator: 0,
            parsing_stats: {},
        },

        // Data
        checkpoint: null,
        dbStats: {},
        activityLog: [],
        error: null,

        // Computed
        get operatorProgress() {
            if (!this.status.total_operators) return 0;
            return Math.round((this.status.current_operator_index / this.status.total_operators) * 100);
        },

        // Initialize
        async init() {
            this.connectWebSocket();
            await this.loadStats();
            await this.loadProgress();

            // Refresh stats periodically
            setInterval(() => this.loadStats(), 30000);
        },

        // WebSocket connection
        connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws/scrape`;

            this.ws = new WebSocket(wsUrl);

            this.ws.onopen = () => {
                this.wsConnected = true;
                this.addLog('info', 'Connected to server');
                if (this.reconnectInterval) {
                    clearInterval(this.reconnectInterval);
                    this.reconnectInterval = null;
                }
            };

            this.ws.onclose = () => {
                this.wsConnected = false;
                this.addLog('error', 'Disconnected from server');
                this.scheduleReconnect();
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };

            this.ws.onmessage = (event) => {
                this.handleMessage(JSON.parse(event.data));
            };

            // Send ping every 30 seconds to keep connection alive
            setInterval(() => {
                if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                    this.ws.send('ping');
                }
            }, 30000);
        },

        scheduleReconnect() {
            if (!this.reconnectInterval) {
                this.reconnectInterval = setInterval(() => {
                    if (!this.wsConnected) {
                        this.connectWebSocket();
                    }
                }, 5000);
            }
        },

        // Handle incoming WebSocket messages
        handleMessage(msg) {
            console.log('WS message:', msg);

            switch (msg.type) {
                case 'connected':
                    if (msg.status) {
                        this.updateStatus(msg.status);
                    }
                    break;

                case 'started':
                    this.isRunning = true;
                    this.sleepPrevented = true;
                    this.addLog('started', `Scrape started: ${msg.config?.source}, max ${msg.config?.max_operators} operators`);
                    break;

                case 'resumed':
                    this.addLog('info', `Resumed: ${msg.processed_operators} operators, ${msg.total_reviews} reviews`);
                    break;

                case 'discovering_operators':
                    this.addLog('info', msg.message);
                    break;

                case 'operators_discovered':
                    this.status.total_operators = msg.to_scrape;
                    this.addLog('operators_discovered', `Found ${msg.total} operators, scraping ${msg.to_scrape}`);
                    break;

                case 'operator_started':
                    this.status.current_operator_index = msg.index;
                    this.status.current_operator = msg.url;
                    this.status.current_page = 1;
                    this.status.reviews_on_current_operator = 0;
                    this.addLog('operator_started', `[${msg.index}/${msg.total}] Starting: ${msg.name || msg.url}`);
                    break;

                case 'page_scraped':
                    this.status.current_page = msg.page;
                    this.status.total_reviews = msg.total_reviews;
                    break;

                case 'operator_completed':
                    this.status.reviews_on_current_operator = msg.reviews_extracted;
                    this.status.total_reviews = msg.total_reviews;
                    if (msg.parsing_stats) {
                        this.status.parsing_stats = msg.parsing_stats;
                    }
                    this.addLog('operator_completed', `[${msg.index}] Completed: ${msg.reviews_extracted} reviews`);
                    break;

                case 'operator_error':
                    this.addLog('error', `[${msg.index}] Error: ${msg.error}`);
                    break;

                case 'captcha_detected':
                    this.addLog('error', 'CAPTCHA detected! Manual intervention required.');
                    this.error = msg.message;
                    break;

                case 'completed':
                    this.isRunning = false;
                    this.sleepPrevented = false;
                    const duration = Math.round(msg.duration_seconds / 60);
                    this.addLog('completed', `Scrape completed: ${msg.total_reviews} reviews in ${duration} minutes`);
                    this.loadStats();
                    break;

                case 'stopping':
                    this.addLog('info', msg.message);
                    break;

                case 'stopped':
                    this.isRunning = false;
                    this.sleepPrevented = false;
                    this.addLog('info', `Scrape stopped: ${msg.total_reviews || 0} reviews`);
                    break;

                case 'error':
                    this.addLog('error', msg.message);
                    if (msg.requires_action) {
                        this.error = msg.message;
                    }
                    break;

                case 'status':
                    this.updateStatus(msg.status);
                    break;
            }
        },

        updateStatus(status) {
            this.isRunning = status.is_running;
            this.sleepPrevented = status.sleep_prevented;
            if (status.is_running) {
                this.status = {
                    current_operator_index: status.current_operator_index || 0,
                    total_operators: status.total_operators || 0,
                    total_reviews: status.total_reviews || 0,
                    current_operator: status.current_operator || '',
                    current_page: status.current_page || 1,
                    reviews_on_current_operator: status.reviews_on_current_operator || 0,
                    parsing_stats: status.parsing_stats || {},
                };
            }
        },

        // API calls
        async startScrape() {
            try {
                const response = await fetch('/api/scrape/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        source: this.config.source,
                        max_operators: this.config.maxOperators,
                        max_reviews_per_operator: this.config.maxReviews,
                        headless: this.config.headless,
                        resume: this.config.resume,
                    }),
                });

                if (!response.ok) {
                    const data = await response.json();
                    throw new Error(data.detail || 'Failed to start scrape');
                }

                this.isRunning = true;
            } catch (err) {
                this.error = err.message;
            }
        },

        async stopScrape() {
            try {
                const response = await fetch('/api/scrape/stop', { method: 'POST' });

                if (!response.ok) {
                    const data = await response.json();
                    throw new Error(data.detail || 'Failed to stop scrape');
                }
            } catch (err) {
                this.error = err.message;
            }
        },

        async clearProgress() {
            try {
                await fetch('/api/scrape/clear', { method: 'POST' });
                this.checkpoint = null;
                this.addLog('info', 'Progress cleared');
            } catch (err) {
                this.error = err.message;
            }
        },

        async loadStats() {
            try {
                const response = await fetch('/api/stats');
                this.dbStats = await response.json();
            } catch (err) {
                console.error('Failed to load stats:', err);
            }
        },

        async loadProgress() {
            try {
                const response = await fetch('/api/progress');
                const data = await response.json();
                if (data.exists) {
                    this.checkpoint = data.data;
                }
            } catch (err) {
                console.error('Failed to load progress:', err);
            }
        },

        // Helpers
        addLog(type, message) {
            this.activityLog.unshift({
                type,
                message,
                timestamp: new Date().toISOString(),
            });

            // Keep only last 100 entries
            if (this.activityLog.length > 100) {
                this.activityLog = this.activityLog.slice(0, 100);
            }
        },

        formatTime(isoString) {
            if (!isoString) return '';
            const date = new Date(isoString);
            return date.toLocaleTimeString('en-US', {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
            });
        },
    };
}
