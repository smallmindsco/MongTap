/**
 * Server Manager Module
 * Manages multiple WellDB/MongoDB server instances
 */

import { MongoDBServer } from '../welldb-node/server/mongodb-server.js';
import { DataFloodStorage } from '../welldb-node/storage/dataflood-storage.js';
import logger from '../utils/logger.js';
import net from 'net';

const log = logger.child('ServerManager');

export class ServerManager {
    constructor(options = {}) {
        this.servers = new Map();
        this.storage = options.storage || new DataFloodStorage();
        this.options = {
            basePort: options.basePort || 27017,
            maxServers: options.maxServers || 10,
            autoAllocatePorts: options.autoAllocatePorts !== false,
            ...options
        };
        this.nextPort = this.options.basePort;
        this.serverCounter = 0;
    }
    
    /**
     * Start a new server instance (alias for createServer)
     */
    async startServer(config = {}) {
        return this.createServer(config);
    }
    
    /**
     * Create and start a new server instance
     */
    async createServer(config = {}) {
        const serverId = config.id || `server-${++this.serverCounter}`;
        
        // Check if server already exists
        if (this.servers.has(serverId)) {
            throw new Error(`Server '${serverId}' already exists`);
        }
        
        // Check max servers limit
        if (this.servers.size >= this.options.maxServers) {
            throw new Error(`Maximum server limit (${this.options.maxServers}) reached`);
        }
        
        // Allocate port
        const port = config.port || (this.options.autoAllocatePorts ? await this.allocatePort() : this.options.basePort);
        
        // Create server configuration
        const serverConfig = {
            port,
            storage: this.storage,
            database: config.database || serverId,
            host: config.host || '127.0.0.1',
            modelName: config.modelName,
            collectionName: config.collectionName || config.modelName,
            ...config
        };
        
        try {
            // Create and start server
            const server = new MongoDBServer(serverConfig);
            await server.start();
            
            // Get actual port (in case port 0 was used)
            const actualPort = server.server.address().port;
            
            // Store server info
            const serverInfo = {
                id: serverId,
                server,
                config: serverConfig,
                port: actualPort,
                database: serverConfig.database,
                modelName: serverConfig.modelName,
                collectionName: serverConfig.collectionName,
                status: 'running',
                createdAt: new Date(),
                startTime: new Date(),
                connections: 0,
                stats: {
                    queries: 0,
                    inserts: 0,
                    updates: 0,
                    deletes: 0
                }
            };
            
            this.servers.set(serverId, serverInfo);
            
            // Set up event listeners
            this.setupServerListeners(serverInfo);
            
            log.info(`Server '${serverId}' started on port ${actualPort}`);
            
            return {
                id: serverId,
                port: actualPort,
                database: serverConfig.database,
                modelName: serverConfig.modelName,
                collectionName: serverConfig.collectionName,
                connectionString: `mongodb://${serverConfig.host}:${actualPort}/${serverConfig.database}`,
                status: 'running'
            };
            
        } catch (error) {
            log.error(`Failed to create server '${serverId}':`, error);
            throw error;
        }
    }
    
    /**
     * Stop a server instance
     */
    async stopServer(serverId) {
        const serverInfo = this.servers.get(serverId);
        
        if (!serverInfo) {
            throw new Error(`Server '${serverId}' not found`);
        }
        
        if (serverInfo.status === 'stopped') {
            return { id: serverId, status: 'already_stopped' };
        }
        
        try {
            await serverInfo.server.stop();
            serverInfo.status = 'stopped';
            serverInfo.stoppedAt = new Date();
            
            log.info(`Server '${serverId}' stopped`);
            
            return {
                id: serverId,
                status: 'stopped',
                stoppedAt: serverInfo.stoppedAt
            };
            
        } catch (error) {
            log.error(`Failed to stop server '${serverId}':`, error);
            throw error;
        }
    }
    
    /**
     * Restart a server instance
     */
    async restartServer(serverId) {
        const serverInfo = this.servers.get(serverId);
        
        if (!serverInfo) {
            throw new Error(`Server '${serverId}' not found`);
        }
        
        log.info(`Restarting server '${serverId}'`);
        
        try {
            // Stop if running
            if (serverInfo.status === 'running') {
                await serverInfo.server.stop();
            }
            
            // Start again
            await serverInfo.server.start();
            
            serverInfo.status = 'running';
            serverInfo.restartedAt = new Date();
            
            return {
                id: serverId,
                status: 'running',
                restartedAt: serverInfo.restartedAt
            };
            
        } catch (error) {
            log.error(`Failed to restart server '${serverId}':`, error);
            throw error;
        }
    }
    
    /**
     * Remove a server instance
     */
    async removeServer(serverId) {
        const serverInfo = this.servers.get(serverId);
        
        if (!serverInfo) {
            throw new Error(`Server '${serverId}' not found`);
        }
        
        // Stop server if running
        if (serverInfo.status === 'running') {
            await this.stopServer(serverId);
        }
        
        // Remove from map
        this.servers.delete(serverId);
        
        log.info(`Server '${serverId}' removed`);
        
        return {
            id: serverId,
            status: 'removed'
        };
    }
    
    /**
     * Get server information
     */
    getServer(serverId) {
        const serverInfo = this.servers.get(serverId);
        
        if (!serverInfo) {
            return null;
        }
        
        return {
            id: serverInfo.id,
            port: serverInfo.port,
            database: serverInfo.database,
            status: serverInfo.status,
            createdAt: serverInfo.createdAt,
            connections: serverInfo.server.connections ? serverInfo.server.connections.size : 0,
            stats: serverInfo.stats
        };
    }
    
    /**
     * List all servers
     */
    listServers() {
        const servers = [];
        
        for (const [id, info] of this.servers) {
            servers.push({
                id: info.id,
                port: info.port,
                database: info.database,
                modelName: info.modelName,
                collectionName: info.collectionName,
                status: info.status,
                createdAt: info.createdAt,
                startTime: info.startTime || info.createdAt,
                connections: info.server.connections ? info.server.connections.size : 0
            });
        }
        
        return servers;
    }
    
    /**
     * Get server by port
     */
    getServerByPort(port) {
        for (const [id, info] of this.servers) {
            if (info.port === port) {
                return this.getServer(id);
            }
        }
        return null;
    }
    
    /**
     * Stop a server by port number
     */
    async stopServerByPort(port) {
        for (const [id, info] of this.servers) {
            if (info.port === port && info.status === 'running') {
                return await this.stopServer(id);
            }
        }
        return null;
    }
    
    /**
     * Stop all servers (alias for stopAll)
     */
    async stopAllServers() {
        return this.stopAll();
    }
    
    /**
     * Stop all servers
     */
    async stopAll() {
        const results = [];
        
        for (const [id, info] of this.servers) {
            if (info.status === 'running') {
                try {
                    await this.stopServer(id);
                    results.push({ id, status: 'stopped' });
                } catch (error) {
                    results.push({ id, status: 'error', error: error.message });
                }
            }
        }
        
        return results;
    }
    
    /**
     * Get overall statistics
     */
    getStatistics() {
        const stats = {
            totalServers: this.servers.size,
            runningServers: 0,
            stoppedServers: 0,
            totalConnections: 0,
            totalOperations: {
                queries: 0,
                inserts: 0,
                updates: 0,
                deletes: 0
            }
        };
        
        for (const info of this.servers.values()) {
            if (info.status === 'running') {
                stats.runningServers++;
                stats.totalConnections += info.server.connections ? info.server.connections.size : 0;
            } else {
                stats.stoppedServers++;
            }
            
            stats.totalOperations.queries += info.stats.queries;
            stats.totalOperations.inserts += info.stats.inserts;
            stats.totalOperations.updates += info.stats.updates;
            stats.totalOperations.deletes += info.stats.deletes;
        }
        
        return stats;
    }
    
    /**
     * Allocate an available port
     */
    async allocatePort() {
        const maxAttempts = 100;
        let attempts = 0;
        
        while (attempts < maxAttempts) {
            const port = this.nextPort++;
            
            // Check if port is already in use by our servers
            const inUse = Array.from(this.servers.values()).some(info => info.port === port);
            if (inUse) {
                attempts++;
                continue;
            }
            
            // Check if port is available on the system
            const available = await this.isPortAvailable(port);
            if (available) {
                return port;
            }
            
            attempts++;
        }
        
        throw new Error('Unable to allocate a free port');
    }
    
    /**
     * Check if a port is available
     */
    isPortAvailable(port) {
        return new Promise((resolve) => {
            const server = net.createServer();
            
            server.once('error', () => {
                resolve(false);
            });
            
            server.once('listening', () => {
                server.close();
                resolve(true);
            });
            
            server.listen(port);
        });
    }
    
    /**
     * Set up event listeners for a server
     */
    setupServerListeners(serverInfo) {
        const server = serverInfo.server;
        
        // Track connections
        if (server.on) {
            server.on('connection', () => {
                serverInfo.connections++;
            });
            
            server.on('query', () => {
                serverInfo.stats.queries++;
            });
            
            server.on('insert', () => {
                serverInfo.stats.inserts++;
            });
            
            server.on('update', () => {
                serverInfo.stats.updates++;
            });
            
            server.on('delete', () => {
                serverInfo.stats.deletes++;
            });
        }
    }
    
    /**
     * Create a server pool with load balancing
     */
    async createServerPool(poolName, size, config = {}) {
        const pool = {
            name: poolName,
            servers: [],
            currentIndex: 0
        };
        
        log.info(`Creating server pool '${poolName}' with ${size} servers`);
        
        for (let i = 0; i < size; i++) {
            const serverId = `${poolName}-${i + 1}`;
            const serverConfig = {
                ...config,
                id: serverId,
                database: config.database || poolName
            };
            
            try {
                const result = await this.createServer(serverConfig);
                pool.servers.push(result);
            } catch (error) {
                log.error(`Failed to create server ${serverId} in pool:`, error);
            }
        }
        
        return pool;
    }
    
    /**
     * Get next server from pool (round-robin)
     */
    getNextFromPool(pool) {
        if (!pool || pool.servers.length === 0) {
            return null;
        }
        
        const server = pool.servers[pool.currentIndex];
        pool.currentIndex = (pool.currentIndex + 1) % pool.servers.length;
        
        return server;
    }
    
    /**
     * Health check for all servers
     */
    async healthCheck() {
        const health = {
            healthy: [],
            unhealthy: [],
            timestamp: new Date()
        };
        
        for (const [id, info] of this.servers) {
            if (info.status === 'running') {
                try {
                    // Simple health check - verify server is listening
                    const isListening = info.server.server && info.server.server.listening;
                    
                    if (isListening) {
                        health.healthy.push({
                            id,
                            port: info.port,
                            connections: info.server.connections ? info.server.connections.size : 0
                        });
                    } else {
                        health.unhealthy.push({
                            id,
                            port: info.port,
                            reason: 'Not listening'
                        });
                    }
                } catch (error) {
                    health.unhealthy.push({
                        id,
                        port: info.port,
                        reason: error.message
                    });
                }
            }
        }
        
        health.summary = {
            total: this.servers.size,
            healthy: health.healthy.length,
            unhealthy: health.unhealthy.length,
            healthRate: this.servers.size > 0 ? health.healthy.length / this.servers.size : 0
        };
        
        return health;
    }
    
    /**
     * Export server configuration
     */
    exportConfiguration() {
        const config = {
            servers: [],
            options: this.options,
            exported: new Date()
        };
        
        for (const [id, info] of this.servers) {
            config.servers.push({
                id: info.id,
                port: info.port,
                database: info.database,
                config: info.config,
                status: info.status
            });
        }
        
        return config;
    }
    
    /**
     * Import server configuration
     */
    async importConfiguration(config) {
        const results = [];
        
        for (const serverConfig of config.servers) {
            try {
                if (!this.servers.has(serverConfig.id)) {
                    await this.createServer(serverConfig.config);
                    results.push({ id: serverConfig.id, status: 'created' });
                } else {
                    results.push({ id: serverConfig.id, status: 'exists' });
                }
            } catch (error) {
                results.push({ id: serverConfig.id, status: 'error', error: error.message });
            }
        }
        
        return results;
    }
}

export default ServerManager;