/**
 * MongoDB Connection Handler
 * Manages individual client connections and message processing
 */

import { EventEmitter } from 'events';
import { OpcodeParser, OpCode } from '../protocol/opcodes.js';
import MessageHandler from '../protocol/message.js';
import { BSON } from '../protocol/bson.js';

/**
 * Connection states
 */
export const ConnectionState = {
    CONNECTING: 'connecting',
    CONNECTED: 'connected',
    AUTHENTICATED: 'authenticated',
    CLOSING: 'closing',
    CLOSED: 'closed',
    ERROR: 'error'
};

/**
 * Individual MongoDB client connection
 */
export class Connection extends EventEmitter {
    constructor(socket, options = {}) {
        super();
        
        // Connection properties
        this.id = options.id || Math.random().toString(36).substring(7);
        this.socket = socket;
        this.remoteAddress = `${socket.remoteAddress}:${socket.remotePort}`;
        this.state = ConnectionState.CONNECTING;
        
        // Configuration
        this.maxMessageSize = options.maxMessageSize || 48000000; // 48MB default
        this.timeout = options.timeout || 300000; // 5 minutes default
        
        // Session properties
        this.database = options.defaultDatabase || 'test';
        this.authenticated = options.requireAuth ? false : true;
        this.username = null;
        this.sessionId = null;
        
        // Message handling
        this.messageHandler = new MessageHandler(options.logger);
        this.buffer = Buffer.alloc(0);
        this.activeMessage = null;
        this.cursors = new Map(); // Track active cursors
        
        // Statistics
        this.stats = {
            connectedAt: new Date(),
            lastActivity: new Date(),
            messagesReceived: 0,
            messagesSent: 0,
            bytesReceived: 0,
            bytesSent: 0,
            errors: 0
        };
        
        // Logging
        this.logger = options.logger || this.createDefaultLogger();
        
        // Setup socket handlers
        this.setupSocketHandlers();
        
        // Set initial state
        this.state = ConnectionState.CONNECTED;
        this.emit('connected');
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
     * Setup socket event handlers
     */
    setupSocketHandlers() {
        // Handle incoming data
        this.socket.on('data', (data) => {
            this.handleData(data);
        });
        
        // Handle socket errors
        this.socket.on('error', (err) => {
            this.handleError(err);
        });
        
        // Handle socket close
        this.socket.on('close', (hadError) => {
            this.handleClose(hadError);
        });
        
        // Handle socket timeout
        this.socket.on('timeout', () => {
            this.handleTimeout();
        });
        
        // Set socket timeout
        if (this.timeout > 0) {
            this.socket.setTimeout(this.timeout);
        }
    }

    /**
     * Handle incoming data
     */
    handleData(data) {
        try {
            // Update statistics
            this.stats.bytesReceived += data.length;
            this.stats.lastActivity = new Date();
            
            // Add to buffer
            this.buffer = Buffer.concat([this.buffer, data]);
            
            // Process complete messages
            this.processBuffer();
            
        } catch (err) {
            this.logger.error(`Connection ${this.id} data handling error:`, err);
            this.handleError(err);
        }
    }

    /**
     * Process buffered data for complete messages
     */
    processBuffer() {
        while (this.buffer.length >= 16) { // Minimum message header size
            // Read message length
            const messageLength = this.buffer.readInt32LE(0);
            
            // Validate message size
            if (messageLength < 16 || messageLength > this.maxMessageSize) {
                throw new Error(`Invalid message size: ${messageLength}`);
            }
            
            // Check if we have complete message
            if (this.buffer.length < messageLength) {
                // Wait for more data
                break;
            }
            
            // Extract complete message
            const messageBuffer = this.buffer.slice(0, messageLength);
            this.buffer = this.buffer.slice(messageLength);
            
            // Process the message
            this.processMessage(messageBuffer);
        }
    }

    /**
     * Process a complete message
     */
    processMessage(messageBuffer) {
        try {
            // Update statistics
            this.stats.messagesReceived++;
            
            // Parse message
            const message = OpcodeParser.parseMessage(messageBuffer);
            
            this.logger.debug(`Connection ${this.id} received ${message.constructor.name}`);
            
            // Emit message event
            this.emit('message', {
                opCode: message.header.opCode,
                requestId: message.header.requestID,
                message
            });
            
            // Store active message for response correlation
            this.activeMessage = message;
            
        } catch (err) {
            this.logger.error(`Connection ${this.id} message parsing error:`, err);
            this.stats.errors++;
            
            // Send error response
            const errorReply = this.messageHandler.createErrorReply(
                0,
                `Message parsing error: ${err.message}`,
                1
            );
            this.send(errorReply);
        }
    }

    /**
     * Send data to client
     */
    send(data) {
        if (this.state === ConnectionState.CLOSED || this.state === ConnectionState.CLOSING) {
            this.logger.warn(`Connection ${this.id} attempted to send while ${this.state}`);
            return false;
        }
        
        try {
            // Write to socket
            this.socket.write(data);
            
            // Update statistics
            this.stats.messagesSent++;
            this.stats.bytesSent += data.length;
            this.stats.lastActivity = new Date();
            
            this.logger.debug(`Connection ${this.id} sent ${data.length} bytes`);
            
            return true;
            
        } catch (err) {
            this.logger.error(`Connection ${this.id} send error:`, err);
            this.handleError(err);
            return false;
        }
    }

    /**
     * Send reply message
     */
    sendReply(documents, cursorId = 0n, flags = 0) {
        if (!this.activeMessage) {
            this.logger.warn(`Connection ${this.id} no active message for reply`);
            return false;
        }
        
        const reply = this.messageHandler.createOpReply(
            this.activeMessage.header.requestID,
            documents,
            cursorId,
            flags
        );
        
        return this.send(reply);
    }

    /**
     * Send error reply
     */
    sendError(errorMessage, errorCode = 1) {
        const requestId = this.activeMessage ? this.activeMessage.header.requestID : 0;
        const errorReply = this.messageHandler.createErrorReply(
            requestId,
            errorMessage,
            errorCode
        );
        
        return this.send(errorReply);
    }

    /**
     * Send success reply
     */
    sendSuccess(result = {}) {
        const requestId = this.activeMessage ? this.activeMessage.header.requestID : 0;
        const successReply = this.messageHandler.createSuccessReply(
            requestId,
            result
        );
        
        return this.send(successReply);
    }

    /**
     * Handle socket error
     */
    handleError(err) {
        this.logger.error(`Connection ${this.id} error:`, err);
        this.stats.errors++;
        this.state = ConnectionState.ERROR;
        this.emit('error', err);
        
        // Close connection on error
        this.close();
    }

    /**
     * Handle socket close
     */
    handleClose(hadError) {
        if (this.state === ConnectionState.CLOSED) {
            return; // Already closed
        }
        
        this.logger.info(`Connection ${this.id} closed${hadError ? ' with error' : ''}`);
        
        // Clear cursors
        this.cursors.clear();
        
        // Update state
        this.state = ConnectionState.CLOSED;
        
        // Clear buffer
        this.buffer = Buffer.alloc(0);
        
        // Emit close event
        this.emit('close', hadError);
    }

    /**
     * Handle socket timeout
     */
    handleTimeout() {
        this.logger.info(`Connection ${this.id} timed out`);
        this.emit('timeout');
        this.close();
    }

    /**
     * Switch database
     */
    useDatabase(database) {
        this.database = database;
        this.logger.info(`Connection ${this.id} switched to database: ${database}`);
        this.emit('databaseChanged', database);
    }

    /**
     * Authenticate connection
     */
    authenticate(username, password) {
        // Simple authentication (can be enhanced)
        this.authenticated = true;
        this.username = username;
        this.state = ConnectionState.AUTHENTICATED;
        
        this.logger.info(`Connection ${this.id} authenticated as ${username}`);
        this.emit('authenticated', username);
        
        return true;
    }

    /**
     * Create a cursor
     */
    createCursor(collection, query, options = {}) {
        const cursorId = BigInt(Date.now()) * 1000n + BigInt(Math.floor(Math.random() * 1000));
        
        const cursor = {
            id: cursorId,
            collection,
            query,
            options,
            position: 0,
            documents: [],
            closed: false,
            createdAt: new Date()
        };
        
        this.cursors.set(cursorId, cursor);
        
        this.logger.debug(`Connection ${this.id} created cursor ${cursorId} for ${collection}`);
        
        return cursorId;
    }

    /**
     * Get cursor by ID
     */
    getCursor(cursorId) {
        return this.cursors.get(cursorId);
    }

    /**
     * Kill cursor
     */
    killCursor(cursorId) {
        if (this.cursors.has(cursorId)) {
            const cursor = this.cursors.get(cursorId);
            cursor.closed = true;
            this.cursors.delete(cursorId);
            
            this.logger.debug(`Connection ${this.id} killed cursor ${cursorId}`);
            return true;
        }
        return false;
    }

    /**
     * Kill all cursors
     */
    killAllCursors() {
        const count = this.cursors.size;
        this.cursors.clear();
        
        if (count > 0) {
            this.logger.debug(`Connection ${this.id} killed ${count} cursors`);
        }
        
        return count;
    }

    /**
     * Close connection
     */
    close() {
        if (this.state === ConnectionState.CLOSED || this.state === ConnectionState.CLOSING) {
            return;
        }
        
        this.state = ConnectionState.CLOSING;
        
        // Kill all cursors
        this.killAllCursors();
        
        // Close socket
        if (this.socket && !this.socket.destroyed) {
            this.socket.end();
            this.socket.destroy();
        }
        
        this.state = ConnectionState.CLOSED;
        
        this.logger.info(`Connection ${this.id} closed`);
    }

    /**
     * Get connection information
     */
    getInfo() {
        return {
            id: this.id,
            remoteAddress: this.remoteAddress,
            state: this.state,
            database: this.database,
            authenticated: this.authenticated,
            username: this.username,
            cursors: this.cursors.size,
            stats: {
                ...this.stats,
                uptime: Date.now() - this.stats.connectedAt.getTime()
            }
        };
    }

    /**
     * Check if connection is alive
     */
    isAlive() {
        return this.state === ConnectionState.CONNECTED || 
               this.state === ConnectionState.AUTHENTICATED;
    }

    /**
     * Reset connection statistics
     */
    resetStats() {
        this.stats.messagesReceived = 0;
        this.stats.messagesSent = 0;
        this.stats.bytesReceived = 0;
        this.stats.bytesSent = 0;
        this.stats.errors = 0;
        this.stats.lastActivity = new Date();
    }
}

// Export connection pool manager
export class ConnectionPool extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.connections = new Map();
        this.maxConnections = options.maxConnections || 1000;
        this.logger = options.logger || console;
        
        // Statistics
        this.stats = {
            totalConnections: 0,
            activeConnections: 0,
            rejectedConnections: 0
        };
    }

    /**
     * Add connection to pool
     */
    add(connection) {
        if (this.connections.size >= this.maxConnections) {
            this.stats.rejectedConnections++;
            this.logger.warn(`Connection pool full, rejecting connection ${connection.id}`);
            connection.close();
            return false;
        }
        
        this.connections.set(connection.id, connection);
        this.stats.totalConnections++;
        this.stats.activeConnections++;
        
        // Handle connection close
        connection.on('close', () => {
            this.remove(connection.id);
        });
        
        this.emit('connectionAdded', connection);
        return true;
    }

    /**
     * Remove connection from pool
     */
    remove(connectionId) {
        if (this.connections.has(connectionId)) {
            const connection = this.connections.get(connectionId);
            this.connections.delete(connectionId);
            this.stats.activeConnections--;
            
            this.emit('connectionRemoved', connection);
            return true;
        }
        return false;
    }

    /**
     * Get connection by ID
     */
    get(connectionId) {
        return this.connections.get(connectionId);
    }

    /**
     * Get all connections
     */
    getAll() {
        return Array.from(this.connections.values());
    }

    /**
     * Broadcast to all connections
     */
    broadcast(data) {
        let sent = 0;
        for (const connection of this.connections.values()) {
            if (connection.send(data)) {
                sent++;
            }
        }
        return sent;
    }

    /**
     * Close all connections
     */
    closeAll() {
        for (const connection of this.connections.values()) {
            connection.close();
        }
        this.connections.clear();
        this.stats.activeConnections = 0;
    }

    /**
     * Get pool statistics
     */
    getStats() {
        return {
            ...this.stats,
            connections: this.getAll().map(conn => conn.getInfo())
        };
    }
}

// Export everything
export default Connection;