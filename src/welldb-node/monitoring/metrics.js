/**
 * Performance Metrics and Monitoring System
 * 
 * Tracks and reports performance metrics for:
 * - Database operations (queries, inserts, updates, deletes)
 * - Model training and generation
 * - Network latency and throughput
 * - Resource usage (memory, connections)
 */

import { EventEmitter } from 'events';
import os from 'os';

/**
 * Metrics collector and reporter
 */
export class MetricsCollector extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.options = {
            collectInterval: options.collectInterval || 10000, // 10 seconds
            historySize: options.historySize || 1000,
            enableSystemMetrics: options.enableSystemMetrics !== false,
            enableDetailedTracking: options.enableDetailedTracking || false
        };
        
        // Operation metrics
        this.operations = {
            queries: new MetricCounter('queries'),
            inserts: new MetricCounter('inserts'),
            updates: new MetricCounter('updates'),
            deletes: new MetricCounter('deletes'),
            aggregations: new MetricCounter('aggregations')
        };
        
        // Model metrics
        this.models = {
            trainings: new MetricCounter('trainings'),
            generations: new MetricCounter('generations'),
            schemaInferences: new MetricCounter('schemaInferences')
        };
        
        // Network metrics
        this.network = {
            connections: new MetricGauge('connections'),
            bytesReceived: new MetricCounter('bytesReceived'),
            bytesSent: new MetricCounter('bytesSent'),
            messagesReceived: new MetricCounter('messagesReceived'),
            messagesSent: new MetricCounter('messagesSent')
        };
        
        // Performance metrics
        this.performance = {
            queryLatency: new MetricHistogram('queryLatency'),
            queriesLatency: new MetricHistogram('queriesLatency'),  // Add plural form
            insertsLatency: new MetricHistogram('insertsLatency'),
            updatesLatency: new MetricHistogram('updatesLatency'),
            deletesLatency: new MetricHistogram('deletesLatency'),
            aggregationsLatency: new MetricHistogram('aggregationsLatency'),
            generationLatency: new MetricHistogram('generationLatency'),
            generationsLatency: new MetricHistogram('generationsLatency'),  // Add plural form
            trainingLatency: new MetricHistogram('trainingLatency'),
            trainingsLatency: new MetricHistogram('trainingsLatency'),  // Add plural form
            schemaInferencesLatency: new MetricHistogram('schemaInferencesLatency'),
            aggregationLatency: new MetricHistogram('aggregationLatency')
        };
        
        // Resource metrics
        this.resources = {
            memoryUsage: new MetricGauge('memoryUsage'),
            cpuUsage: new MetricGauge('cpuUsage'),
            activeCollections: new MetricGauge('activeCollections'),
            activeCursors: new MetricGauge('activeCursors'),
            modelCacheSize: new MetricGauge('modelCacheSize')
        };
        
        // Error metrics
        this.errors = {
            total: new MetricCounter('errors'),
            byType: new Map()
        };
        
        // Start time
        this.startTime = Date.now();
        
        // History
        this.history = [];
        
        // Start collection if enabled
        if (this.options.collectInterval > 0) {
            this.startCollection();
        }
        
        // Logging
        this.logger = options.logger || this.createDefaultLogger();
    }
    
    createDefaultLogger() {
        return {
            debug: () => {},
            info: console.error,
            warn: console.warn,
            error: console.error
        };
    }
    
    /**
     * Record an operation
     */
    recordOperation(type, duration, success = true, metadata = {}) {
        const operation = this.operations[type];
        if (operation) {
            operation.increment();
            
            
            if (duration !== undefined) {
                // Try both plural and singular forms for latency metrics
                let latencyMetric = this.performance[`${type}Latency`];
                if (!latencyMetric) {
                    // Try singular form (queries -> query, inserts -> insert, etc.)
                    let singular;
                    if (type === 'queries') {
                        singular = 'query';
                    } else if (type.endsWith('ies')) {
                        singular = type.slice(0, -3) + 'y';
                    } else if (type.endsWith('es')) {
                        singular = type.slice(0, -2);
                    } else if (type.endsWith('s')) {
                        singular = type.slice(0, -1);
                    } else {
                        singular = type;
                    }
                    latencyMetric = this.performance[`${singular}Latency`];
                }
                if (latencyMetric) {
                    latencyMetric.record(duration);
                }
            }
        }
        
        if (!success) {
            this.recordError(type, metadata.error);
        }
        
        if (this.options.enableDetailedTracking) {
            this.emit('operation', {
                type,
                duration,
                success,
                metadata,
                timestamp: Date.now()
            });
        }
    }
    
    /**
     * Record model operation
     */
    recordModelOperation(type, duration, metadata = {}) {
        const operation = this.models[type];
        if (operation) {
            operation.increment();
            
            // Try both plural and singular forms for latency metrics
            let latencyMetric = this.performance[`${type}Latency`];
            if (!latencyMetric) {
                latencyMetric = this.performance[`${type.slice(0, -1)}Latency`];
            }
            if (latencyMetric && duration !== undefined) {
                latencyMetric.record(duration);
            }
        }
        
        if (this.options.enableDetailedTracking) {
            this.emit('modelOperation', {
                type,
                duration,
                metadata,
                timestamp: Date.now()
            });
        }
    }
    
    /**
     * Record network activity
     */
    recordNetwork(type, value) {
        const metric = this.network[type];
        if (metric) {
            if (metric instanceof MetricCounter) {
                metric.increment(value);
            } else {
                metric.set(value);
            }
        }
    }
    
    /**
     * Record resource usage
     */
    recordResource(type, value) {
        const metric = this.resources[type];
        if (metric) {
            metric.set(value);
        }
    }
    
    /**
     * Record an error
     */
    recordError(type, error) {
        this.errors.total.increment();
        
        // Use the type parameter as the error type, not error.name
        const errorType = type || error?.name || 'Unknown';
        if (!this.errors.byType.has(errorType)) {
            this.errors.byType.set(errorType, new MetricCounter(errorType));
        }
        this.errors.byType.get(errorType).increment();
        
        this.emit('error', {
            type: errorType,
            message: error?.message,
            timestamp: Date.now()
        });
    }
    
    /**
     * Start periodic collection
     */
    startCollection() {
        this.collectionInterval = setInterval(() => {
            this.collect();
        }, this.options.collectInterval);
    }
    
    /**
     * Stop periodic collection
     */
    stopCollection() {
        if (this.collectionInterval) {
            clearInterval(this.collectionInterval);
            this.collectionInterval = null;
        }
    }
    
    /**
     * Collect current metrics
     */
    collect() {
        const snapshot = this.getSnapshot();
        
        // Add to history
        this.history.push(snapshot);
        if (this.history.length > this.options.historySize) {
            this.history.shift();
        }
        
        // Emit snapshot
        this.emit('snapshot', snapshot);
        
        return snapshot;
    }
    
    /**
     * Get current metrics snapshot
     */
    getSnapshot() {
        const snapshot = {
            timestamp: Date.now(),
            uptime: Date.now() - this.startTime,
            
            operations: {
                queries: this.operations.queries.getValue(),
                inserts: this.operations.inserts.getValue(),
                updates: this.operations.updates.getValue(),
                deletes: this.operations.deletes.getValue(),
                aggregations: this.operations.aggregations.getValue()
            },
            
            models: {
                trainings: this.models.trainings.getValue(),
                generations: this.models.generations.getValue(),
                schemaInferences: this.models.schemaInferences.getValue()
            },
            
            network: {
                connections: this.network.connections.getValue(),
                bytesReceived: this.network.bytesReceived.getValue(),
                bytesSent: this.network.bytesSent.getValue(),
                messagesReceived: this.network.messagesReceived.getValue(),
                messagesSent: this.network.messagesSent.getValue()
            },
            
            performance: {
                queryLatency: this.performance.queryLatency.getStats(),
                generationLatency: this.performance.generationLatency.getStats(),
                trainingLatency: this.performance.trainingLatency.getStats(),
                aggregationLatency: this.performance.aggregationLatency.getStats()
            },
            
            resources: {
                memoryUsage: this.resources.memoryUsage.getValue(),
                cpuUsage: this.resources.cpuUsage.getValue(),
                activeCollections: this.resources.activeCollections.getValue(),
                activeCursors: this.resources.activeCursors.getValue(),
                modelCacheSize: this.resources.modelCacheSize.getValue()
            },
            
            errors: {
                total: this.errors.total.getValue(),
                byType: Object.fromEntries(
                    Array.from(this.errors.byType.entries()).map(([k, v]) => [k, v.getValue()])
                )
            }
        };
        
        // Add system metrics if enabled
        if (this.options.enableSystemMetrics) {
            snapshot.system = this.getSystemMetrics();
        }
        
        return snapshot;
    }
    
    /**
     * Get system metrics
     */
    getSystemMetrics() {
        const memUsage = process.memoryUsage();
        const cpus = os.cpus();
        
        return {
            memory: {
                rss: memUsage.rss,
                heapTotal: memUsage.heapTotal,
                heapUsed: memUsage.heapUsed,
                external: memUsage.external,
                systemTotal: os.totalmem(),
                systemFree: os.freemem()
            },
            cpu: {
                cores: cpus.length,
                model: cpus[0]?.model,
                usage: this.calculateCPUUsage(cpus)
            },
            process: {
                pid: process.pid,
                version: process.version,
                uptime: process.uptime()
            }
        };
    }
    
    /**
     * Calculate CPU usage percentage
     */
    calculateCPUUsage(cpus) {
        let totalIdle = 0;
        let totalTick = 0;
        
        for (const cpu of cpus) {
            for (const type in cpu.times) {
                totalTick += cpu.times[type];
            }
            totalIdle += cpu.times.idle;
        }
        
        return ((totalTick - totalIdle) / totalTick) * 100;
    }
    
    /**
     * Get performance report
     */
    getReport() {
        const current = this.getSnapshot();
        const rates = this.calculateRates();
        
        return {
            summary: {
                uptime: this.formatDuration(current.uptime),
                totalOperations: Object.values(current.operations).reduce((a, b) => a + b, 0),
                totalErrors: current.errors.total,
                errorRate: rates.errorRate
            },
            
            throughput: {
                operationsPerSecond: rates.opsPerSecond,
                queriesPerSecond: rates.queriesPerSecond,
                generationsPerSecond: rates.generationsPerSecond
            },
            
            latency: {
                query: current.performance.queryLatency,
                generation: current.performance.generationLatency,
                training: current.performance.trainingLatency,
                aggregation: current.performance.aggregationLatency
            },
            
            resources: current.resources,
            
            topErrors: Array.from(this.errors.byType.entries())
                .sort((a, b) => b[1].getValue() - a[1].getValue())
                .slice(0, 5)
                .map(([type, counter]) => ({
                    type,
                    count: counter.getValue()
                }))
        };
    }
    
    /**
     * Calculate rates from history
     */
    calculateRates() {
        if (this.history.length < 2) {
            return {
                opsPerSecond: 0,
                queriesPerSecond: 0,
                generationsPerSecond: 0,
                errorRate: 0
            };
        }
        
        const recent = this.history[this.history.length - 1];
        const previous = this.history[this.history.length - 2];
        const timeDiff = (recent.timestamp - previous.timestamp) / 1000;
        
        const totalOps = Object.values(recent.operations).reduce((a, b) => a + b, 0);
        const prevTotalOps = Object.values(previous.operations).reduce((a, b) => a + b, 0);
        
        return {
            opsPerSecond: (totalOps - prevTotalOps) / timeDiff,
            queriesPerSecond: (recent.operations.queries - previous.operations.queries) / timeDiff,
            generationsPerSecond: (recent.models.generations - previous.models.generations) / timeDiff,
            errorRate: recent.errors.total / totalOps || 0
        };
    }
    
    /**
     * Format duration for display
     */
    formatDuration(ms) {
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);
        
        if (days > 0) return `${days}d ${hours % 24}h`;
        if (hours > 0) return `${hours}h ${minutes % 60}m`;
        if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
        return `${seconds}s`;
    }
    
    /**
     * Reset all metrics
     */
    reset() {
        for (const metric of Object.values(this.operations)) {
            metric.reset();
        }
        for (const metric of Object.values(this.models)) {
            metric.reset();
        }
        for (const metric of Object.values(this.network)) {
            metric.reset();
        }
        for (const metric of Object.values(this.performance)) {
            metric.reset();
        }
        for (const metric of Object.values(this.resources)) {
            metric.reset();
        }
        
        this.errors.total.reset();
        this.errors.byType.clear();
        this.history = [];
        this.startTime = Date.now();
    }
    
    /**
     * Destroy the collector
     */
    destroy() {
        this.stopCollection();
        this.removeAllListeners();
    }
}

/**
 * Metric counter (increments only)
 */
class MetricCounter {
    constructor(name) {
        this.name = name;
        this.value = 0;
    }
    
    increment(amount = 1) {
        this.value += amount;
    }
    
    getValue() {
        return this.value;
    }
    
    reset() {
        this.value = 0;
    }
}

/**
 * Metric gauge (can go up or down)
 */
class MetricGauge {
    constructor(name) {
        this.name = name;
        this.value = 0;
    }
    
    set(value) {
        this.value = value;
    }
    
    increment(amount = 1) {
        this.value += amount;
    }
    
    decrement(amount = 1) {
        this.value -= amount;
    }
    
    getValue() {
        return this.value;
    }
    
    reset() {
        this.value = 0;
    }
}

/**
 * Metric histogram (tracks distribution)
 */
class MetricHistogram {
    constructor(name) {
        this.name = name;
        this.values = [];
        this.sum = 0;
        this.count = 0;
        this.min = Infinity;
        this.max = -Infinity;
    }
    
    record(value) {
        this.values.push(value);
        this.sum += value;
        this.count++;
        this.min = Math.min(this.min, value);
        this.max = Math.max(this.max, value);
        
        // Keep only recent values (last 1000)
        if (this.values.length > 1000) {
            const removed = this.values.shift();
            this.sum -= removed;
        }
    }
    
    getStats() {
        if (this.count === 0) {
            return {
                count: 0,
                min: 0,
                max: 0,
                mean: 0,
                median: 0,
                p95: 0,
                p99: 0
            };
        }
        
        const sorted = [...this.values].sort((a, b) => a - b);
        
        return {
            count: this.count,
            min: this.min,
            max: this.max,
            mean: this.sum / this.count,
            median: sorted[Math.floor(sorted.length / 2)],
            p95: sorted[Math.floor(sorted.length * 0.95)],
            p99: sorted[Math.floor(sorted.length * 0.99)]
        };
    }
    
    reset() {
        this.values = [];
        this.sum = 0;
        this.count = 0;
        this.min = Infinity;
        this.max = -Infinity;
    }
}

/**
 * Global metrics instance
 */
let globalMetrics = null;

/**
 * Get or create global metrics collector
 */
export function getMetrics(options) {
    if (!globalMetrics) {
        globalMetrics = new MetricsCollector(options);
    }
    return globalMetrics;
}

/**
 * Reset global metrics
 */
export function resetMetrics() {
    if (globalMetrics) {
        globalMetrics.reset();
    }
}

export default MetricsCollector;