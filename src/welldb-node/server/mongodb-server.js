/**
 * MongoDB Wire Protocol Server
 * TCP server that handles MongoDB protocol connections
 * Integrates with DataFlood for model-based data generation
 */

import net from 'net';
import { EventEmitter } from 'events';
import MessageHandler from '../protocol/message.js';
import { OpcodeParser, OpCode } from '../protocol/opcodes.js';
import DataFloodStorage from '../storage/dataflood-storage.js';
import { BSON, Long } from '../protocol/bson.js';
import { CRUDHandlers } from './crud-handlers.js';
import config from '../../config/config-loader.js';

/**
 * MongoDB-compatible server with DataFlood integration
 */
export class MongoDBServer extends EventEmitter {
    constructor(options = {}) {
        super();
        
        // Store options for later use
        this.options = options;
        
        // Server configuration
        this.port = options.port || 27017;
        this.host = options.host || '127.0.0.1';
        this.maxConnections = options.maxConnections || 1000;
        
        // Components
        this.server = null;
        this.connections = new Map();
        this.messageHandler = new MessageHandler(options.logger);
        
        // Use provided storage or create new one
        this.storage = options.storage || new DataFloodStorage({
            basePath: options.modelPath || config.storage.modelsBasePath,
            logger: options.logger,
            enableAutoTrain: options.enableAutoTrain !== false || config.server.enableAutoTrain,
            trainThreshold: options.trainThreshold || config.server.trainThreshold
        });
        
        // CRUD handlers with DataFlood integration
        this.crudHandlers = new CRUDHandlers({
            storage: this.storage,
            messageHandler: this.messageHandler,
            logger: options.logger
        });
        
        // Logging
        this.logger = options.logger || this.createDefaultLogger();
        
        // Statistics
        this.stats = {
            connectionsTotal: 0,
            connectionsActive: 0,
            messagesReceived: 0,
            messagesSent: 0,
            bytesReceived: 0,
            bytesSent: 0,
            startTime: null
        };
        
        // Connection ID counter
        this.connectionIdCounter = 1;
    }

    createDefaultLogger() {
        return {
            debug: () => {},
            info: () => {},      // Suppress in stdio mode
            warn: () => {},      // Suppress in stdio mode  
            error: () => {}      // Suppress errors - should never output to stderr
        };
    }

    /**
     * Start the MongoDB server
     */
    async start() {
        // Initialize storage only if it wasn't provided
        if (!this.options?.storage) {
            await this.storage.initialize();
        }
        
        return new Promise((resolve, reject) => {
            this.server = net.createServer();
            
            // Set max connections
            this.server.maxConnections = this.maxConnections;
            
            // Handle new connections
            this.server.on('connection', (socket) => {
                this.handleConnection(socket);
            });
            
            // Handle server errors
            this.server.on('error', (err) => {
                this.logger.error('Server error:', err);
                this.emit('error', err);
                reject(err);
            });
            
            // Start listening
            this.server.listen(this.port, this.host, () => {
                this.stats.startTime = new Date();
                this.logger.info(`MongoDB server listening on ${this.host}:${this.port}`);
                this.emit('listening', { host: this.host, port: this.port });
                resolve();
            });
        });
    }

    /**
     * Stop the MongoDB server
     */
    async stop() {
        return new Promise((resolve) => {
            // Close all connections
            for (const conn of this.connections.values()) {
                conn.socket.end();
            }
            this.connections.clear();
            
            // Close server
            if (this.server) {
                this.server.close(() => {
                    this.logger.info('MongoDB server stopped');
                    this.emit('closed');
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }

    /**
     * Handle a new client connection
     */
    handleConnection(socket) {
        const connectionId = this.connectionIdCounter++;
        const remoteAddress = `${socket.remoteAddress}:${socket.remotePort}`;
        
        this.logger.info(`New connection ${connectionId} from ${remoteAddress}`);
        
        // Create connection object
        const connection = {
            id: connectionId,
            socket,
            remoteAddress,
            buffer: Buffer.alloc(0),
            database: 'test',  // Default database
            authenticated: true, // No auth for now
            startTime: new Date(),
            stats: {
                messagesReceived: 0,
                messagesSent: 0,
                bytesReceived: 0,
                bytesSent: 0
            }
        };
        
        // Store connection
        this.connections.set(connectionId, connection);
        this.stats.connectionsTotal++;
        this.stats.connectionsActive++;
        
        // Emit connection event
        this.emit('connection', { id: connectionId, remoteAddress });
        
        // Handle socket events
        socket.on('data', (data) => {
            this.handleData(connection, data);
        });
        
        socket.on('error', (err) => {
            this.logger.error(`Connection ${connectionId} error:`, err);
            this.emit('connectionError', { id: connectionId, error: err });
        });
        
        socket.on('close', () => {
            this.logger.info(`Connection ${connectionId} closed`);
            this.connections.delete(connectionId);
            this.stats.connectionsActive--;
            this.emit('connectionClosed', { id: connectionId });
        });
    }

    /**
     * Handle incoming data from a connection
     */
    handleData(connection, data) {
        // Add to buffer
        connection.buffer = Buffer.concat([connection.buffer, data]);
        connection.stats.bytesReceived += data.length;
        this.stats.bytesReceived += data.length;
        
        // Process complete messages
        while (connection.buffer.length >= 16) {  // Minimum message size
            // Check if we have a complete message
            const messageLength = connection.buffer.readInt32LE(0);
            
            if (connection.buffer.length < messageLength) {
                // Wait for more data
                break;
            }
            
            // Extract message
            const messageBuffer = connection.buffer.slice(0, messageLength);
            connection.buffer = connection.buffer.slice(messageLength);
            
            // Process message
            this.processMessage(connection, messageBuffer);
        }
    }

    /**
     * Process a complete MongoDB wire protocol message
     */
    async processMessage(connection, messageBuffer) {
        connection.stats.messagesReceived++;
        this.stats.messagesReceived++;
        
        try {
            // Parse message
            const message = OpcodeParser.parseMessage(messageBuffer);
            const requestId = message.header.requestID;
            
            this.logger.debug(`Processing ${message.constructor.name} from connection ${connection.id}`);
            
            // Route based on opcode
            let response;
            switch (message.header.opCode) {
                case OpCode.OP_QUERY:
                    response = await this.handleQuery(connection, message);
                    break;
                    
                case OpCode.OP_INSERT:
                    response = await this.handleInsert(connection, message);
                    break;
                    
                case OpCode.OP_UPDATE:
                    response = await this.handleUpdate(connection, message);
                    break;
                    
                case OpCode.OP_DELETE:
                    response = await this.handleDelete(connection, message);
                    break;
                    
                case OpCode.OP_GET_MORE:
                    response = await this.handleGetMore(connection, message);
                    break;
                    
                case OpCode.OP_KILL_CURSORS:
                    response = await this.handleKillCursors(connection, message);
                    break;
                    
                case OpCode.OP_MSG:
                    response = await this.handleMsg(connection, message);
                    break;
                    
                default:
                    this.logger.warn(`Unsupported opcode: ${message.header.opCode}`);
                    response = this.messageHandler.createErrorReply(
                        requestId,
                        `Unsupported operation: ${message.header.opCode}`,
                        1
                    );
            }
            
            // Send response if one was generated
            if (response) {
                this.sendResponse(connection, response);
            }
            
        } catch (err) {
            this.logger.error(`Error processing message:`, err);
            const errorResponse = this.messageHandler.createErrorReply(
                0,
                err.message,
                1
            );
            this.sendResponse(connection, errorResponse);
        }
    }

    /**
     * Handle OP_QUERY message
     */
    async handleQuery(connection, message) {
        const { fullCollectionName, numberToSkip, numberToReturn } = message;
        
        // Emit query event for statistics
        this.emit('query', { connectionId: connection.id, collection: fullCollectionName });
        
        // Parse query from BSON buffer if present
        let query = {};
        let returnFieldsSelector = {};
        
        
        if (message.queryBuffer && message.queryBuffer.length > 0) {
            this.logger.debug('Query buffer length:', message.queryBuffer.length);
            try {
                // First BSON document is the query
                const querySize = message.queryBuffer.readInt32LE(0);
                if (querySize > 0 && querySize <= message.queryBuffer.length) {
                    const queryDoc = message.queryBuffer.slice(0, querySize);
                    query = BSON.deserialize(queryDoc);
                    
                    // Second BSON document (if present) is the projection
                    if (message.queryBuffer.length > querySize) {
                        const projBuffer = message.queryBuffer.slice(querySize);
                        const projSize = projBuffer.readInt32LE(0);
                        if (projSize > 0 && projSize <= projBuffer.length) {
                            returnFieldsSelector = BSON.deserialize(projBuffer.slice(0, projSize));
                        }
                    }
                }
            } catch (err) {
                this.logger.error('Failed to parse query BSON:', err);
            }
        }
        
        // Use pre-parsed values only if they have actual content
        // Don't overwrite with empty objects from the default constructor
        if (message.query !== undefined && Object.keys(message.query).length > 0) {
            query = message.query;
        }
        if (message.returnFieldsSelector !== undefined && message.returnFieldsSelector !== null) {
            returnFieldsSelector = message.returnFieldsSelector;
        }
        this.logger.info(`Query on ${fullCollectionName}:`, JSON.stringify(query));
        
        // Handle special collections
        const [database, collection] = fullCollectionName.split('.');
        if (collection === '$cmd') {
            return this.handleCommand(connection, database, query, message.header.requestID);
        }
        
        try {
            // Use CRUD handlers for query
            const result = await this.crudHandlers.handleFind(
                fullCollectionName,
                query,
                {
                    skip: numberToSkip,
                    limit: numberToReturn,
                    projection: returnFieldsSelector
                }
            );
            
            // Create response
            return this.messageHandler.createOpReply(
                message.header.requestID,
                result.documents,
                result.cursorId
            );
        } catch (error) {
            this.logger.error('Query error:', error);
            return this.messageHandler.createErrorReply(
                message.header.requestID,
                error.message,
                1
            );
        }
    }

    /**
     * Handle OP_INSERT message
     */
    async handleInsert(connection, message) {
        const { fullCollectionName, flags } = message;
        
        // Emit insert event for statistics
        this.emit('insert', { connectionId: connection.id, collection: fullCollectionName });
        
        // Parse documents from BSON buffer if present
        let documents = [];
        
        if (message.documentsBuffer && message.documentsBuffer.length > 0) {
            try {
                let offset = 0;
                const buffer = message.documentsBuffer;
                
                while (offset < buffer.length) {
                    const docSize = buffer.readInt32LE(offset);
                    if (docSize <= 0 || offset + docSize > buffer.length) break;
                    
                    const docBuffer = buffer.slice(offset, offset + docSize);
                    const doc = BSON.deserialize(docBuffer);
                    documents.push(doc);
                    offset += docSize;
                }
            } catch (err) {
                this.logger.error('Failed to parse insert documents:', err);
            }
        }
        
        // Fallback to pre-parsed documents if available
        if (message.documents && message.documents.length > 0) {
            documents = message.documents;
        }
        
        this.logger.info(`Insert ${documents.length} documents into ${fullCollectionName}`);
        
        try {
            // Use CRUD handlers for insert
            await this.crudHandlers.handleInsert(fullCollectionName, documents, flags);
            
            // No response for OP_INSERT (fire and forget)
            return null;
        } catch (error) {
            this.logger.error('Insert error:', error);
            // Still return null as OP_INSERT doesn't get responses
            return null;
        }
    }

    /**
     * Handle OP_UPDATE message
     */
    async handleUpdate(connection, message) {
        const { fullCollectionName, selector, update, flags } = message;
        
        // Emit update event for statistics
        this.emit('update', { connectionId: connection.id, collection: fullCollectionName });
        
        this.logger.info(`Update in ${fullCollectionName}:`, { selector, update });
        
        try {
            // Use CRUD handlers for update
            await this.crudHandlers.handleUpdate(fullCollectionName, selector, update, flags);
            
            // No response for OP_UPDATE (fire and forget)
            return null;
        } catch (error) {
            this.logger.error('Update error:', error);
            return null;
        }
    }

    /**
     * Handle OP_DELETE message
     */
    async handleDelete(connection, message) {
        const { fullCollectionName, selector, flags } = message;
        
        // Emit delete event for statistics
        this.emit('delete', { connectionId: connection.id, collection: fullCollectionName });
        
        this.logger.info(`Delete from ${fullCollectionName}:`, selector);
        
        try {
            // Use CRUD handlers for delete
            await this.crudHandlers.handleDelete(fullCollectionName, selector, flags);
            
            // No response for OP_DELETE (fire and forget)
            return null;
        } catch (error) {
            this.logger.error('Delete error:', error);
            return null;
        }
    }

    /**
     * Handle OP_GET_MORE message
     */
    async handleGetMore(connection, message) {
        const { fullCollectionName, cursorID, numberToReturn } = message;
        
        this.logger.info(`Get more from cursor ${cursorID} in ${fullCollectionName}`);
        
        // No cursor support yet, return empty
        return this.messageHandler.createOpReply(
            message.header.requestID,
            [],
            0n
        );
    }

    /**
     * Handle OP_KILL_CURSORS message
     */
    async handleKillCursors(connection, message) {
        const { cursorIDs } = message;
        
        this.logger.info(`Kill cursors:`, cursorIDs);
        
        // No response for OP_KILL_CURSORS
        return null;
    }

    /**
     * Handle OP_MSG message (modern protocol)
     */
    async handleMsg(connection, message) {
        // Extract command from first section
        if (!message.sections || message.sections.length === 0) {
            return this.messageHandler.createErrorReply(
                message.header.requestID,
                'Invalid OP_MSG: no sections',
                1
            );
        }
        
        const command = message.sections[0].document;
        if (!command) {
            return this.messageHandler.createErrorReply(
                message.header.requestID,
                'Invalid OP_MSG: no command document',
                1
            );
        }
        
        this.logger.info('OP_MSG command:', command);
        
        // Route based on command
        if (command.insert) {
            return this.handleInsertCommand(connection, command, message.header.requestID);
        } else if (command.find) {
            return this.handleFindCommand(connection, command, message.header.requestID);
        } else if (command.update) {
            return this.handleUpdateCommand(connection, command, message.header.requestID);
        } else if (command.delete) {
            return this.handleDeleteCommand(connection, command, message.header.requestID);
        } else if (command.aggregate) {
            return this.handleAggregateCommand(connection, command, message.header.requestID);
        } else if (command.count) {
            return this.handleCountCommand(connection, command, message.header.requestID);
        } else if (command.createIndexes) {
            return this.handleCreateIndexCommand(connection, command, message.header.requestID);
        } else if (command.listIndexes) {
            return this.handleListIndexesCommand(connection, command, message.header.requestID);
        } else {
            return this.handleCommand(connection, command.$db || connection.database, command, message.header.requestID);
        }
    }

    /**
     * Handle database commands
     */
    async handleCommand(connection, database, command, requestId) {
        this.logger.info(`Command on ${database}:`, command);
        
        // Handle isMaster/hello command
        if (command.isMaster || command.ismaster || command.hello) {
            return this.messageHandler.createOpMsg({
                ok: 1,
                ismaster: true,
                maxBsonObjectSize: 16777216,
                maxMessageSizeBytes: 48000000,
                maxWriteBatchSize: 100000,
                localTime: new Date(),
                minWireVersion: 0,
                maxWireVersion: 13,
                readOnly: false,
                // Session support
                logicalSessionTimeoutMinutes: 30,
                connectionId: Math.floor(Math.random() * 1000000),
                // Topology information
                msg: 'isdbgrid',
                topologyVersion: {
                    processId: { $oid: '507f1f77bcf86cd799439011' },
                    counter: 6
                },
                // Additional capabilities
                compression: ['snappy', 'zlib'],
                saslSupportedMechs: []
            }, 0, requestId);
        }
        
        // Handle ping
        if (command.ping) {
            return this.messageHandler.createOpMsg({ ok: 1 }, 0, requestId);
        }
        
        // Handle startSession
        if (command.startSession) {
            const sessionId = {
                id: { 
                    $binary: {
                        base64: Buffer.from(Math.random().toString()).toString('base64'),
                        subType: '04'
                    }
                }
            };
            return this.messageHandler.createOpMsg({
                ok: 1,
                ...sessionId
            }, 0, requestId);
        }
        
        // Handle endSessions
        if (command.endSessions) {
            return this.messageHandler.createOpMsg({ ok: 1 }, 0, requestId);
        }
        
        // Handle refreshSessions
        if (command.refreshSessions) {
            return this.messageHandler.createOpMsg({ ok: 1 }, 0, requestId);
        }
        
        // Handle connectionStatus
        if (command.connectionStatus) {
            const response = {
                authInfo: {
                    authenticatedUsers: [],
                    authenticatedUserRoles: []
                },
                ok: 1
            };
            
            // Add privileges if requested
            if (command.showPrivileges) {
                response.authInfo.authenticatedUserPrivileges = [];
            }
            
            return this.messageHandler.createOpMsg(response, 0, requestId);
        }
        
        // Handle getParameter
        if (command.getParameter) {
            const response = { ok: 1 };
            
            if (command.featureCompatibilityVersion) {
                response.featureCompatibilityVersion = {
                    version: "7.0",
                    targetVersion: "7.0"
                };
            }
            
            return this.messageHandler.createOpMsg(response, 0, requestId);
        }
        
        // Handle atlasVersion (MongoDB Atlas specific)
        if (command.atlasVersion) {
            // Not Atlas, return command not found
            return this.messageHandler.createOpMsg({
                ok: 0,
                errmsg: "Command not found: atlasVersion",
                code: 59
            }, 0, requestId);
        }
        
        // Handle buildInfo
        if (command.buildInfo) {
            return this.messageHandler.createOpMsg({
                version: "7.0.0",
                gitVersion: "mongtap",
                modules: [],
                allocator: "system",
                javascriptEngine: "none",
                sysInfo: "MongTap/DataFlood",
                versionArray: [7, 0, 0, 0],
                ok: 1
            }, 0, requestId);
        }
        
        // Handle hostInfo
        if (command.hostInfo) {
            return this.messageHandler.createOpMsg({
                system: {
                    currentTime: new Date(),
                    hostname: "localhost",
                    cpuAddrSize: 64,
                    memSizeMB: 8192,
                    numCores: 4,
                    cpuArch: "arm64"
                },
                ok: 1
            }, 0, requestId);
        }
        
        // Handle listDatabases
        if (command.listDatabases) {
            const databases = await this.storage.listDatabases();
            return this.messageHandler.createOpMsg({
                databases: databases.map(name => ({
                    name,
                    sizeOnDisk: 0,
                    empty: false
                })),
                totalSize: 0,
                ok: 1
            }, 0, requestId);
        }
        
        // Handle listCollections
        if (command.listCollections) {
            let collections = await this.storage.listCollections(database);
            
            // For configured default database, also show available models as collections
            if (database === config.storage.defaultDatabase) {
                // Get available models from the default database directory
                const modelCollections = await this.storage.listCollections(config.storage.defaultDatabase);
                // Merge with any existing collections
                const allCollections = new Set([...collections, ...modelCollections]);
                collections = Array.from(allCollections);
            }
            
            // Create properly formatted cursor response
            const cursorDoc = {
                id: new Long(0, 0),  // Use BSON Long type for cursor ID
                ns: `${database}.$cmd.listCollections`,
                firstBatch: collections.map(name => ({
                    name,
                    type: 'collection',
                    options: {},
                    info: {
                        readOnly: false,
                        uuid: null
                    },
                    idIndex: {
                        v: 2,
                        key: { _id: 1 },
                        name: '_id_'
                    }
                }))
            };
            
            return this.messageHandler.createOpMsg({
                cursor: cursorDoc,
                ok: 1
            }, 0, requestId);
        }
        
        // Handle getLastError
        if (command.getLastError || command.getlasterror) {
            return this.messageHandler.createOpMsg({
                ok: 1,
                err: null,
                n: 0
            }, 0, requestId);
        }
        
        // Unknown command
        return this.messageHandler.createOpMsg({
            ok: 0,
            errmsg: `Unknown command: ${Object.keys(command).join(', ')}`,
            code: 59
        }, 0, requestId);
    }

    /**
     * Handle insert command (OP_MSG style)
     */
    async handleInsertCommand(connection, command, requestId) {
        try {
            const result = await this.crudHandlers.handleInsertCommand(command);
            return this.messageHandler.createOpMsg(result, 0, requestId);
        } catch (error) {
            this.logger.error('Insert command error:', error);
            return this.messageHandler.createOpMsg({
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            }, 0, requestId);
        }
    }

    /**
     * Handle find command (OP_MSG style)
     */
    async handleFindCommand(connection, command, requestId) {
        try {
            const result = await this.crudHandlers.handleFindCommand(command);
            // Use createOpMsg for OP_MSG responses, not createOpReply
            return this.messageHandler.createOpMsg(result, 0, requestId);
        } catch (error) {
            this.logger.error('Find command error:', error);
            // Error responses still use createOpMsg
            return this.messageHandler.createOpMsg({
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            }, 0, requestId);
        }
    }
    

    /**
     * Handle update command (OP_MSG style)
     */
    async handleUpdateCommand(connection, command, requestId) {
        try {
            const result = await this.crudHandlers.handleUpdateCommand(command);
            return this.messageHandler.createOpMsg(result, 0, requestId);
        } catch (error) {
            this.logger.error('Update command error:', error);
            return this.messageHandler.createOpMsg({
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            }, 0, requestId);
        }
    }

    /**
     * Handle delete command (OP_MSG style)
     */
    async handleDeleteCommand(connection, command, requestId) {
        try {
            const result = await this.crudHandlers.handleDeleteCommand(command);
            return this.messageHandler.createOpMsg(result, 0, requestId);
        } catch (error) {
            this.logger.error('Delete command error:', error);
            return this.messageHandler.createOpMsg({
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            }, 0, requestId);
        }
    }
    
    /**
     * Handle aggregate command (OP_MSG style)
     */
    async handleAggregateCommand(connection, command, requestId) {
        try {
            const result = await this.crudHandlers.handleAggregateCommand(command);
            return this.messageHandler.createOpMsg(result, 0, requestId);
        } catch (error) {
            this.logger.error('Aggregate command error:', error);
            return this.messageHandler.createOpMsg({
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            }, 0, requestId);
        }
    }
    
    /**
     * Handle count command (OP_MSG style)
     */
    async handleCountCommand(connection, command, requestId) {
        try {
            const result = await this.crudHandlers.handleCountCommand(command);
            return this.messageHandler.createOpMsg(result, 0, requestId);
        } catch (error) {
            this.logger.error('Count command error:', error);
            return this.messageHandler.createOpMsg({
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            }, 0, requestId);
        }
    }
    
    /**
     * Handle createIndexes command (OP_MSG style)
     */
    async handleCreateIndexCommand(connection, command, requestId) {
        try {
            const result = await this.crudHandlers.handleCreateIndexCommand(command);
            return this.messageHandler.createOpMsg(result, 0, requestId);
        } catch (error) {
            this.logger.error('CreateIndex command error:', error);
            return this.messageHandler.createErrorReply(requestId, error.message, 1);
        }
    }
    
    /**
     * Handle listIndexes command (OP_MSG style)
     */
    async handleListIndexesCommand(connection, command, requestId) {
        try {
            const result = await this.crudHandlers.handleListIndexesCommand(command);
            return this.messageHandler.createOpMsg(result, 0, requestId);
        } catch (error) {
            this.logger.error('ListIndexes command error:', error);
            return this.messageHandler.createErrorReply(requestId, error.message, 1);
        }
    }

    /**
     * Extract constraints from MongoDB query for generation
     */
    extractConstraints(query) {
        const constraints = {};
        
        if (!query || typeof query !== 'object') {
            return constraints;
        }
        
        // Extract simple equality constraints
        for (const [field, value] of Object.entries(query)) {
            if (!field.startsWith('$') && !isOperatorObject(value)) {
                constraints[field] = value;
            }
        }
        
        return constraints;
        
        function isOperatorObject(value) {
            return value && typeof value === 'object' && 
                   Object.keys(value).some(k => k.startsWith('$'));
        }
    }

    /**
     * Filter documents based on MongoDB query
     * Basic implementation - can be enhanced
     */
    filterDocuments(documents, query) {
        if (!query || Object.keys(query).length === 0) {
            return documents;
        }
        
        return documents.filter(doc => this.matchesQuery(doc, query));
    }

    /**
     * Check if a document matches a query
     */
    matchesQuery(doc, query) {
        for (const [field, condition] of Object.entries(query)) {
            const value = this.getFieldValue(doc, field);
            
            // Handle operators
            if (typeof condition === 'object' && condition !== null) {
                if (!this.matchesOperators(value, condition)) {
                    return false;
                }
            } else {
                // Simple equality
                if (value !== condition) {
                    return false;
                }
            }
        }
        
        return true;
    }

    /**
     * Get nested field value from document
     */
    getFieldValue(doc, path) {
        const parts = path.split('.');
        let current = doc;
        
        for (const part of parts) {
            if (current == null) return undefined;
            current = current[part];
        }
        
        return current;
    }

    /**
     * Match value against operator conditions
     */
    matchesOperators(value, operators) {
        for (const [op, operand] of Object.entries(operators)) {
            switch (op) {
                case '$eq':
                    if (value !== operand) return false;
                    break;
                case '$ne':
                    if (value === operand) return false;
                    break;
                case '$gt':
                    if (!(value > operand)) return false;
                    break;
                case '$gte':
                    if (!(value >= operand)) return false;
                    break;
                case '$lt':
                    if (!(value < operand)) return false;
                    break;
                case '$lte':
                    if (!(value <= operand)) return false;
                    break;
                case '$in':
                    if (!operand.includes(value)) return false;
                    break;
                case '$nin':
                    if (operand.includes(value)) return false;
                    break;
                case '$exists':
                    if ((value !== undefined) !== operand) return false;
                    break;
                default:
                    // Unknown operator, ignore for now
                    break;
            }
        }
        
        return true;
    }

    /**
     * Send response to client
     */
    sendResponse(connection, responseBuffer) {
        connection.socket.write(responseBuffer);
        connection.stats.messagesSent++;
        connection.stats.bytesSent += responseBuffer.length;
        this.stats.messagesSent++;
        this.stats.bytesSent += responseBuffer.length;
        
        this.logger.debug(`Sent ${responseBuffer.length} bytes to connection ${connection.id}`);
    }

    /**
     * Get server statistics
     */
    getStats() {
        return {
            ...this.stats,
            uptime: this.stats.startTime ? Date.now() - this.stats.startTime.getTime() : 0,
            connections: Array.from(this.connections.values()).map(conn => ({
                id: conn.id,
                remoteAddress: conn.remoteAddress,
                database: conn.database,
                uptime: Date.now() - conn.startTime.getTime(),
                stats: conn.stats
            }))
        };
    }
}

// Export as default
export default MongoDBServer;