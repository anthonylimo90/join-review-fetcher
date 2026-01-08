/**
 * Safari Review Scraper - Alpine.js Application
 */
function scraperApp() {
    return {
        // Tab navigation
        activeTab: 'dashboard',

        // WebSocket
        ws: null,
        wsConnected: false,
        reconnectInterval: null,

        // Modals
        showNewScrapeModal: false,

        // Scrape Preview
        scrapePreview: null,
        previewLoading: false,

        // Config
        config: {
            source: 'safaribookings',
            maxOperators: 50,
            maxReviews: 200,
            headless: true,
            resume: true,
            parallelWorkers: 4,  // NEW: Parallel scraping workers
        },

        // Scraper Status
        isRunning: false,
        isPaused: false,  // NEW: Pause state for pause/resume feature
        sleepPrevented: false,
        status: {
            current_operator_index: 0,
            total_operators: 0,
            total_reviews: 0,
            current_operator: '',
            current_page: 1,
            reviews_on_current_operator: 0,
            parsing_stats: {},
            config: null,
        },

        // Discovered operators during scrape
        discoveredOperators: [],
        operatorFilter: 'all',

        // Activity
        activityLog: [],
        activityExpanded: false,

        // Run history
        runHistory: [],

        // Data
        checkpoint: null,
        dbStats: {},
        error: null,

        // NEW: Phase tracking
        currentPhase: 'idle', // 'idle' | 'discovering' | 'scraping' | 'completed'

        // NEW: Time tracking
        elapsedSeconds: 0,
        elapsedTimer: null,

        // NEW: Health monitoring
        health: {
            networkRetries: 0,
            rateLimitMultiplier: 1.0,
            operatorsSinceRestart: 0,
        },

        // NEW: CAPTCHA state
        captchaDetected: false,
        captchaDetectedAt: null,
        captchaDismissed: false,

        // NEW: Operator stats
        operatorStats: {
            success: 0,
            error: 0,
            skipped: 0,
        },

        // NEW: Guide tracking
        guidesMentionedCount: 0,

        // NEW: Parallel scraping tracking
        activeWorkers: [],  // Currently scraping operators
        totalWorkers: 4,    // Configured parallel workers

        // Operators tab
        operators: {
            data: [],
            total: 0,
            offset: 0,
            limit: 20,
            search: '',
            sort: 'reviews',
        },
        operatorsList: [], // for filter dropdown

        // Reviews tab
        reviews: {
            data: [],
            total: 0,
            offset: 0,
            limit: 20,
            search: '',
            operator: '',
            country: '',
            source: '',
        },
        countriesList: [],
        expandedReview: null,

        // Analysis
        analysis: {
            guides: null,
            guideIntelligence: null,
            guideIntelligenceLoading: false,
        },
        isRefreshingAnalysis: false,

        // Export
        exportConfig: {
            format: 'csv',
            reviews: true,
            guideAnalysis: false,
            demographics: false,
            decisionFactors: false,
        },

        // Computed
        get operatorProgress() {
            if (!this.status.total_operators) return 0;
            return Math.round((this.status.current_operator_index / this.status.total_operators) * 100);
        },

        get currentOperatorName() {
            if (!this.status.current_operator) return '';
            const url = this.status.current_operator;
            return url.split('/').pop() || url;
        },

        get filteredOperators() {
            if (this.operatorFilter === 'all') return this.discoveredOperators;
            return this.discoveredOperators.filter(op => op.status === this.operatorFilter);
        },

        get estimatedTimeRemaining() {
            if (!this.isRunning || !this.status.total_operators) return null;
            const remaining = this.status.total_operators - (this.status.current_operator_index || 0);
            if (remaining <= 0) return null;

            // Use actual speed if available, otherwise estimate based on parallel workers
            const opsPerHour = this.operatorsPerHour;
            let minsRemaining;

            if (opsPerHour > 0) {
                const hoursRemaining = remaining / opsPerHour;
                minsRemaining = hoursRemaining * 60;
            } else {
                // Fallback: ~0.15 minutes per operator with parallel scraping (was 0.55)
                // Adjusted for 4x parallel + resource blocking + reduced delays
                const baseTimePerOp = 0.55 / this.totalWorkers;  // ~0.14 min with 4 workers
                minsRemaining = remaining * baseTimePerOp;
            }

            const hours = Math.floor(minsRemaining / 60);
            const mins = Math.floor(minsRemaining % 60);

            if (hours > 0) {
                return `~${hours}h ${mins}m`;
            } else if (mins > 0) {
                return `~${mins}m`;
            }
            return '< 1m';
        },

        // NEW: Elapsed time formatted as HH:MM:SS
        get elapsedTimeFormatted() {
            const hours = Math.floor(this.elapsedSeconds / 3600);
            const minutes = Math.floor((this.elapsedSeconds % 3600) / 60);
            const seconds = this.elapsedSeconds % 60;
            return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
        },

        // NEW: Reviews per hour
        get reviewsPerHour() {
            if (!this.elapsedSeconds || this.elapsedSeconds < 60) return 0;
            const hours = this.elapsedSeconds / 3600;
            return Math.round((this.status.total_reviews || 0) / hours);
        },

        // NEW: Operators per hour
        get operatorsPerHour() {
            if (!this.elapsedSeconds || this.elapsedSeconds < 60) return 0;
            const hours = this.elapsedSeconds / 3600;
            return parseFloat(((this.status.current_operator_index || 0) / hours).toFixed(1));
        },

        // NEW: Parsing success rate
        get parsingSuccessRate() {
            const stats = this.status.parsing_stats;
            if (!stats || !stats.successful) return 100;
            const total = (stats.successful || 0) + (stats.failed || 0);
            if (total === 0) return 100;
            return ((stats.successful / total) * 100).toFixed(1);
        },

        // NEW: Operator breakdown percentages
        get operatorSuccessPercent() {
            const total = this.operatorStats.success + this.operatorStats.error + this.operatorStats.skipped;
            return total > 0 ? (this.operatorStats.success / total) * 100 : 0;
        },

        get operatorErrorPercent() {
            const total = this.operatorStats.success + this.operatorStats.error + this.operatorStats.skipped;
            return total > 0 ? (this.operatorStats.error / total) * 100 : 0;
        },

        get operatorSkippedPercent() {
            const total = this.operatorStats.success + this.operatorStats.error + this.operatorStats.skipped;
            return total > 0 ? (this.operatorStats.skipped / total) * 100 : 0;
        },

        // NEW: Average reviews per operator
        get avgReviewsPerOperator() {
            const ops = this.operatorStats.success;
            if (!ops) return 0;
            return ((this.status.total_reviews || 0) / ops).toFixed(1);
        },

        // NEW: CAPTCHA time remaining
        get captchaTimeRemaining() {
            if (!this.captchaDetectedAt) return '';
            const elapsed = Math.floor((Date.now() - this.captchaDetectedAt) / 1000);
            const timeout = 600; // 10 minutes
            const remaining = timeout - elapsed;
            if (remaining <= 0) return 'Timeout!';
            const mins = Math.floor(remaining / 60);
            const secs = remaining % 60;
            return `${mins}:${secs.toString().padStart(2, '0')}`;
        },

        // NEW: Browser restart countdown
        get browserRestartCountdown() {
            return 50 - (this.health.operatorsSinceRestart % 50);
        },

        // NEW: Active workers count
        get activeWorkersCount() {
            return this.activeWorkers.length;
        },

        // Initialize
        async init() {
            this.connectWebSocket();
            await this.loadStats();
            await this.loadProgress();
            await this.loadRunHistory();
            await this.loadGuideAnalysis();

            // Load data for current tab
            this.$watch('activeTab', async (tab) => {
                if (tab === 'operators') await this.loadOperators();
                if (tab === 'reviews') await this.loadReviews();
                if (tab === 'analysis') {
                    await this.loadGuideAnalysis();
                    await this.loadGuideIntelligence();
                }
            });

            // Refresh stats periodically (but respect cache - server caches for 5 min)
            setInterval(() => this.loadStats(), 60000);

            // Poll status only when running AND WebSocket is disconnected
            // (WebSocket provides real-time updates when connected)
            setInterval(() => {
                if (this.isRunning && !this.wsConnected) {
                    this.pollStatus();
                }
            }, 3000);
        },

        async pollStatus() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                if (data.scraper) {
                    this.updateStatus(data.scraper);
                }
            } catch (err) {
                console.error('Status poll error:', err);
            }
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
                if (event.data === 'pong') return;
                try {
                    this.handleMessage(JSON.parse(event.data));
                } catch (e) {
                    console.log('Non-JSON message:', event.data);
                }
            };

            // Ping every 30 seconds
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
                    this.discoveredOperators = [];
                    // NEW: Reset and initialize scrape state
                    this.resetScrapeState();
                    this.currentPhase = 'discovering';
                    this.startElapsedTimer();
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
                    // NEW: Transition to scraping phase
                    this.currentPhase = 'scraping';
                    // Initialize discovered operators list
                    if (msg.operator_urls) {
                        this.discoveredOperators = msg.operator_urls.map(url => ({
                            url,
                            name: url.split('/').pop() || url,
                            status: 'pending',
                            reviews: 0
                        }));
                    }
                    const skippedMsg = msg.skipped_full ? `, ${msg.skipped_full} fully scraped` : '';
                    this.addLog('operators_discovered', `Found ${msg.total} operators, scraping ${msg.to_scrape} (${msg.already_done || 0} in progress${skippedMsg})`);
                    break;

                case 'checking_database':
                    this.addLog('info', msg.message);
                    break;

                case 'parallel_scraping':
                    // NEW: Parallel scraping mode started
                    this.totalWorkers = msg.workers || 4;
                    this.addLog('info', `Parallel scraping with ${this.totalWorkers} workers`);
                    break;

                case 'operator_started':
                    this.status.current_operator_index = msg.index;
                    this.status.current_operator = msg.url;
                    this.status.current_page = 1;
                    this.status.reviews_on_current_operator = 0;
                    // NEW: Track operators since last browser restart
                    this.health.operatorsSinceRestart++;
                    // NEW: Track active workers for parallel scraping
                    if (!this.activeWorkers.includes(msg.url)) {
                        this.activeWorkers.push(msg.url);
                    }
                    // Update discovered operators list
                    this.updateOperatorStatus(msg.url, 'running');
                    this.addLog('operator_started', `[${msg.index}/${msg.total}] Starting: ${msg.name || msg.url}`);
                    break;

                case 'page_scraped':
                    this.status.current_page = msg.page;
                    this.status.total_reviews = msg.total_reviews;
                    break;

                case 'operator_completed':
                    this.status.reviews_on_current_operator = msg.new_reviews || msg.reviews_scraped || 0;
                    this.status.total_reviews = msg.total_reviews;
                    this.status.current_operator_index = msg.index;  // Update progress
                    if (msg.parsing_stats) {
                        this.status.parsing_stats = msg.parsing_stats;
                    }
                    // NEW: Track operator success
                    this.operatorStats.success++;
                    // NEW: Track guide mentions if provided
                    if (msg.guides_mentioned) {
                        this.guidesMentionedCount += msg.guides_mentioned;
                    }
                    // NEW: Remove from active workers
                    this.activeWorkers = this.activeWorkers.filter(url => url !== msg.url);
                    // Update discovered operators list
                    this.updateOperatorStatus(msg.url, 'completed', msg.new_reviews || msg.reviews_scraped);
                    // Show new vs duplicate info
                    const newCount = msg.new_reviews || 0;
                    const dupCount = msg.duplicates_skipped || 0;
                    const logMsg = dupCount > 0
                        ? `[${msg.index}] Completed: ${newCount} new, ${dupCount} duplicates skipped`
                        : `[${msg.index}] Completed: ${newCount} reviews`;
                    this.addLog('operator_completed', logMsg);
                    break;

                case 'operator_error':
                    // NEW: Track operator error
                    this.operatorStats.error++;
                    // NEW: Remove from active workers
                    this.activeWorkers = this.activeWorkers.filter(url => url !== msg.url);
                    this.updateOperatorStatus(msg.url, 'error');
                    this.addLog('error', `[${msg.index}] Error: ${msg.error}`);
                    break;

                case 'captcha_detected':
                    // NEW: Track CAPTCHA detection
                    this.captchaDetected = true;
                    this.captchaDetectedAt = Date.now();
                    this.captchaDismissed = false;
                    this.addLog('error', 'CAPTCHA detected! Manual intervention required.');
                    this.error = msg.message;
                    break;

                case 'completed':
                    this.isRunning = false;
                    this.sleepPrevented = false;
                    // NEW: Stop timer and set phase
                    this.stopElapsedTimer();
                    this.currentPhase = 'completed';
                    this.captchaDetected = false;
                    const duration = Math.round(msg.duration_seconds / 60);
                    this.addLog('completed', `Scrape completed: ${msg.total_reviews} reviews in ${duration} minutes`);
                    this.loadStats();
                    this.loadRunHistory();
                    break;

                case 'stopping':
                    this.addLog('info', msg.message);
                    break;

                case 'stopped':
                    this.isRunning = false;
                    this.sleepPrevented = false;
                    // NEW: Stop timer and reset phase
                    this.stopElapsedTimer();
                    this.currentPhase = 'idle';
                    this.captchaDetected = false;
                    this.addLog('info', `Scrape stopped: ${msg.total_reviews || 0} reviews`);
                    this.loadRunHistory();
                    break;

                // NEW: Pause handlers for pause/resume feature
                case 'pausing':
                    this.addLog('info', msg.message || 'Pausing scrape...');
                    break;

                case 'paused':
                    this.isRunning = false;
                    this.isPaused = true;
                    this.sleepPrevented = false;
                    this.stopElapsedTimer();
                    this.currentPhase = 'paused';
                    this.captchaDetected = false;
                    this.addLog('info', msg.message || 'Scrape paused');
                    this.loadRunHistory();
                    break;

                case 'error':
                    this.addLog('error', msg.message);
                    if (msg.requires_action) {
                        this.error = msg.message;
                    }
                    break;

                // NEW: Browser restart handler
                case 'browser_restart':
                    this.health.operatorsSinceRestart = 0;
                    this.addLog('info', msg.message || 'Browser restarted');
                    break;

                // NEW: Rate limit change handler
                case 'rate_limit_change':
                    this.health.rateLimitMultiplier = msg.multiplier || 1.0;
                    if (msg.multiplier > 1.5) {
                        this.addLog('warning', `Rate limiting detected, delay multiplier: ${msg.multiplier.toFixed(1)}x`);
                    }
                    break;

                // NEW: CAPTCHA solved handler
                case 'captcha_solved':
                    this.captchaDetected = false;
                    this.captchaDetectedAt = null;
                    this.captchaDismissed = false;
                    this.addLog('info', 'CAPTCHA solved, resuming...');
                    break;

                // NEW: Network retry handler
                case 'network_retry':
                    this.health.networkRetries++;
                    this.addLog('warning', `Network retry (${this.health.networkRetries}): ${msg.error || 'Connection error'}`);
                    break;

                case 'status':
                    this.updateStatus(msg.status);
                    break;
            }
        },

        updateOperatorStatus(url, status, reviews = 0) {
            const op = this.discoveredOperators.find(o => o.url === url);
            if (op) {
                op.status = status;
                if (reviews) op.reviews = reviews;
            }
        },

        // NEW: Timer methods
        startElapsedTimer() {
            this.stopElapsedTimer(); // Clear any existing
            this.elapsedSeconds = 0;
            this.elapsedTimer = setInterval(() => {
                this.elapsedSeconds++;
            }, 1000);
        },

        stopElapsedTimer() {
            if (this.elapsedTimer) {
                clearInterval(this.elapsedTimer);
                this.elapsedTimer = null;
            }
        },

        // NEW: CAPTCHA methods
        dismissCaptchaAlert() {
            this.captchaDismissed = true;
        },

        // NEW: Phase helper
        isPhaseComplete(phaseId) {
            const phaseOrder = ['idle', 'discovering', 'scraping', 'completed'];
            const currentIdx = phaseOrder.indexOf(this.currentPhase);
            const checkIdx = phaseOrder.indexOf(phaseId);
            return checkIdx < currentIdx && checkIdx > 0;
        },

        // NEW: Reset scrape state
        resetScrapeState() {
            this.currentPhase = 'idle';
            this.elapsedSeconds = 0;
            this.isPaused = false;  // Clear pause state when starting new scrape
            this.operatorStats = { success: 0, error: 0, skipped: 0 };
            this.health = { networkRetries: 0, rateLimitMultiplier: 1.0, operatorsSinceRestart: 0 };
            this.captchaDetected = false;
            this.captchaDetectedAt = null;
            this.captchaDismissed = false;
            this.guidesMentionedCount = 0;
            this.activeWorkers = [];  // Clear active workers
        },

        updateStatus(status) {
            this.isRunning = status.is_running;
            this.sleepPrevented = status.sleep_prevented;
            this.status = {
                current_operator_index: status.current_operator_index || 0,
                total_operators: status.total_operators || 0,
                total_reviews: status.total_reviews || 0,
                current_operator: status.current_operator || '',
                current_page: status.current_page || 1,
                reviews_on_current_operator: status.reviews_on_current_operator || 0,
                parsing_stats: status.parsing_stats || {},
                config: status.config || null,
            };

            if (status.errors && status.errors.length > 0) {
                status.errors.forEach(err => {
                    if (!this.activityLog.find(l => l.message === err)) {
                        this.addLog('error', err);
                    }
                });
            }
        },

        // API calls
        async startScrape() {
            try {
                // Just pass user's requested count - scraper_runner handles resume logic
                const response = await fetch('/api/scrape/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        source: this.config.source,
                        max_operators: this.config.maxOperators,
                        max_reviews_per_operator: this.config.maxReviews,
                        headless: this.config.headless,
                        resume: this.config.resume,
                        parallel_workers: this.config.parallelWorkers,  // NEW: Parallel workers
                    }),
                });

                if (!response.ok) {
                    const data = await response.json();
                    throw new Error(data.detail || 'Failed to start scrape');
                }

                this.isRunning = true;
                this.discoveredOperators = [];
                this.totalWorkers = this.config.parallelWorkers;  // Store configured workers
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

        async pauseScrape() {
            try {
                const response = await fetch('/api/scrape/pause', { method: 'POST' });
                if (!response.ok) {
                    const data = await response.json();
                    throw new Error(data.detail || 'Failed to pause scrape');
                }
                // isPaused will be set by WebSocket 'paused' event
            } catch (err) {
                this.error = err.message;
            }
        },

        async clearProgress() {
            try {
                await fetch('/api/scrape/clear', { method: 'POST' });
                this.checkpoint = null;
                this.scrapePreview = null;
                this.addLog('info', 'Progress cleared');
                // Reload preview if modal is open
                if (this.showNewScrapeModal) {
                    await this.loadScrapePreview();
                }
            } catch (err) {
                this.error = err.message;
            }
        },

        async loadScrapePreview() {
            this.previewLoading = true;
            try {
                const params = new URLSearchParams({
                    source: this.config.source,
                    max_operators: this.config.maxOperators,
                    resume: this.config.resume,
                });
                const response = await fetch(`/api/scrape/preview?${params}`);
                if (response.ok) {
                    this.scrapePreview = await response.json();
                }
            } catch (err) {
                console.error('Failed to load scrape preview:', err);
            } finally {
                this.previewLoading = false;
            }
        },

        async openNewScrapeModal() {
            this.showNewScrapeModal = true;
            await this.loadScrapePreview();
        },

        async updatePreviewDebounced() {
            // Debounce preview updates when config changes
            if (this._previewTimeout) clearTimeout(this._previewTimeout);
            this._previewTimeout = setTimeout(() => {
                this.loadScrapePreview();
            }, 300);
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

        async loadRunHistory() {
            try {
                const response = await fetch('/api/runs');
                if (response.ok) {
                    const data = await response.json();
                    this.runHistory = data.runs || [];
                }
            } catch (err) {
                console.error('Failed to load run history:', err);
            }
        },

        async loadOperators() {
            try {
                const params = new URLSearchParams({
                    limit: this.operators.limit,
                    offset: this.operators.offset,
                    sort: this.operators.sort,
                });
                if (this.operators.search) params.set('search', this.operators.search);

                const response = await fetch(`/api/operators?${params}`);
                const data = await response.json();
                this.operators.data = data.operators || [];
                this.operators.total = data.total || 0;

                // Also update operators list for filter dropdown if empty
                if (this.operatorsList.length === 0) {
                    this.operatorsList = this.operators.data.map(op => op.operator_name);
                }
            } catch (err) {
                console.error('Failed to load operators:', err);
            }
        },

        async loadReviews() {
            try {
                const params = new URLSearchParams({
                    limit: this.reviews.limit,
                    offset: this.reviews.offset,
                });
                if (this.reviews.search) params.set('search', this.reviews.search);
                if (this.reviews.operator) params.set('operator', this.reviews.operator);
                if (this.reviews.country) params.set('country', this.reviews.country);
                if (this.reviews.source) params.set('source', this.reviews.source);

                const response = await fetch(`/api/reviews?${params}`);
                const data = await response.json();
                this.reviews.data = data.reviews || [];
                this.reviews.total = data.total || this.dbStats.total_reviews || 0;

                // Load countries list if empty
                if (this.countriesList.length === 0) {
                    await this.loadCountries();
                }
                // Load operators list if empty
                if (this.operatorsList.length === 0) {
                    await this.loadOperatorsList();
                }
            } catch (err) {
                console.error('Failed to load reviews:', err);
            }
        },

        async loadCountries() {
            try {
                const response = await fetch('/api/countries');
                if (response.ok) {
                    const data = await response.json();
                    this.countriesList = data.countries || [];
                }
            } catch (err) {
                console.error('Failed to load countries:', err);
            }
        },

        async loadOperatorsList() {
            try {
                const response = await fetch('/api/operators?limit=1000');
                if (response.ok) {
                    const data = await response.json();
                    this.operatorsList = (data.operators || []).map(op => op.operator_name);
                }
            } catch (err) {
                console.error('Failed to load operators list:', err);
            }
        },

        async loadGuideAnalysis() {
            try {
                const response = await fetch('/api/analysis/guides');
                if (response.ok) {
                    this.analysis.guides = await response.json();
                }
            } catch (err) {
                console.error('Failed to load guide analysis:', err);
            }
        },

        async loadGuideIntelligence() {
            if (this.analysis.guideIntelligence) return; // Already loaded
            this.analysis.guideIntelligenceLoading = true;
            try {
                const response = await fetch('/api/analysis/guide-intelligence');
                if (response.ok) {
                    this.analysis.guideIntelligence = await response.json();
                }
            } catch (err) {
                console.error('Failed to load guide intelligence:', err);
            } finally {
                this.analysis.guideIntelligenceLoading = false;
            }
        },

        async refreshAnalysis() {
            this.isRefreshingAnalysis = true;
            try {
                // Clear the cache on server
                await fetch('/api/analysis/refresh', { method: 'POST' });
                // Clear local cached data
                this.analysis.guides = null;
                this.analysis.guideIntelligence = null;
                // Reload fresh data
                await this.loadStats();
                await this.loadGuideAnalysis();
                // Force reload guide intelligence (bypass the early return)
                this.analysis.guideIntelligenceLoading = true;
                const response = await fetch('/api/analysis/guide-intelligence');
                if (response.ok) {
                    this.analysis.guideIntelligence = await response.json();
                }
                this.analysis.guideIntelligenceLoading = false;
                this.addLog('info', 'Analysis refreshed');
            } catch (err) {
                this.error = err.message;
                console.error('Failed to refresh analysis:', err);
            } finally {
                this.isRefreshingAnalysis = false;
            }
        },

        viewOperatorReviews(operatorName) {
            this.reviews.operator = operatorName;
            this.reviews.offset = 0;
            this.activeTab = 'reviews';
            this.loadReviews();
        },

        async downloadExport() {
            try {
                const params = new URLSearchParams({
                    format: this.exportConfig.format,
                });
                if (this.exportConfig.reviews) params.set('reviews', 'true');
                if (this.exportConfig.guideAnalysis) params.set('guide_analysis', 'true');
                if (this.exportConfig.demographics) params.set('demographics', 'true');
                if (this.exportConfig.decisionFactors) params.set('decision_factors', 'true');

                window.location.href = `/api/export/${this.exportConfig.format}?${params}`;
            } catch (err) {
                this.error = err.message;
            }
        },

        // Helpers
        addLog(type, message) {
            this.activityLog.unshift({
                type,
                message,
                timestamp: new Date().toISOString(),
            });
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

        formatRunDate(isoString) {
            if (!isoString) return '-';
            const date = new Date(isoString);
            const now = new Date();
            const diffDays = Math.floor((now - date) / (1000 * 60 * 60 * 24));

            if (diffDays === 0) {
                return 'Today ' + date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
            } else if (diffDays === 1) {
                return 'Yesterday';
            } else {
                return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            }
        },

        formatDuration(startIso, endIso) {
            if (!startIso) return '-';
            const start = new Date(startIso);
            const end = endIso ? new Date(endIso) : new Date();
            const diffMs = end - start;
            const diffMins = Math.floor(diffMs / 60000);
            const diffHours = Math.floor(diffMins / 60);
            const mins = diffMins % 60;

            if (diffHours > 0) {
                return `${diffHours}h ${mins}m`;
            }
            return `${diffMins}m`;
        },
    };
}
