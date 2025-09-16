/**
 * Enhanced MCP Server Implementation
 * Fully compliant with Model Context Protocol specification
 * Includes tools, prompts, and resources
 */

import { EventEmitter } from 'events';
import net from 'net';
import readline from 'readline';
import { PromptAnalyzer } from './prompt-analyzer.js';
import { SampleGenerator } from './sample-generator.js';
import { SchemaInferrer } from '../dataflood-js/schema/inferrer.js';
import { DocumentGenerator } from '../dataflood-js/generator/document-generator.js';
import { MongoDBServer } from '../welldb-node/server/mongodb-server.js';
import { DataFloodStorage } from '../welldb-node/storage/dataflood-storage.js';
import fs from 'fs';
import path from 'path';
import config from '../config/config-loader.js';

export class MCPServerEnhanced extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.options = {
            transport: options.transport || 'stdio',
            port: options.port || config.server.defaultPort || 3000,
            host: options.host || config.server.host || 'localhost',
            modelsPath: options.modelsPath || config.storage.modelsBasePath,
            ...options
        };
        
        this.logger = options.logger || {
            debug: () => {},
            info: () => {},     // Suppress info in stdio mode
            warn: () => {},     // Suppress warn in stdio mode  
            error: () => {}     // Suppress errors in stdio mode - they should never go to stderr
        };
        
        // Initialize components
        this.promptAnalyzer = new PromptAnalyzer();
        this.sampleGenerator = new SampleGenerator();
        this.inferrer = new SchemaInferrer();
        this.generator = new DocumentGenerator();
        this.storage = new DataFloodStorage({ 
            basePath: this.options.modelsPath,
            logger: this.logger 
        });
        
        // Server state
        this.servers = new Map();
        this.models = new Map();
        this.startTime = Date.now();
        
        // Define available tools
        this.tools = [
            {
                name: 'generateDataModel',
                description: 'Generate a DataFlood model from sample data or description',
                inputSchema: {
                    type: 'object',
                    properties: {
                        name: { type: 'string', description: 'Name for the model' },
                        description: { type: 'string', description: 'Natural language description of the data structure' },
                        samples: { type: 'array', description: 'Sample documents to train the model', items: { type: 'object' } }
                    },
                    required: ['name']
                }
            },
            {
                name: 'startMongoServer',
                description: 'Start a MongoDB-compatible server with DataFlood backing',
                inputSchema: {
                    type: 'object',
                    properties: {
                        port: { type: 'integer', description: 'Port to listen on (0 for auto)', default: 0 },
                        database: { type: 'string', description: 'Default database name', default: 'test' }
                    }
                }
            },
            {
                name: 'stopMongoServer',
                description: 'Stop a running MongoDB server',
                inputSchema: {
                    type: 'object',
                    properties: {
                        port: { type: 'integer', description: 'Port of the server to stop' }
                    },
                    required: ['port']
                }
            },
            {
                name: 'listActiveServers',
                description: 'List all active MongoDB servers',
                inputSchema: {
                    type: 'object',
                    properties: {}
                }
            },
            {
                name: 'queryModel',
                description: 'Query a DataFlood model directly. Supports generation control via $seed and $entropy parameters in the query.',
                inputSchema: {
                    type: 'object',
                    properties: {
                        model: { type: 'string', description: 'Model name' },
                        query: { 
                            type: 'object', 
                            description: 'MongoDB-style query. Special parameters: $seed (number) for reproducible generation, $entropy (0-1) to control randomness level' 
                        },
                        count: { type: 'integer', description: 'Number of documents to generate', default: 10 }
                    },
                    required: ['model']
                }
            },
            {
                name: 'trainModel',
                description: 'Train or update a model with new data',
                inputSchema: {
                    type: 'object',
                    properties: {
                        model: { type: 'string', description: 'Model name' },
                        documents: { type: 'array', description: 'Documents to train with', items: { type: 'object' } }
                    },
                    required: ['model', 'documents']
                }
            },
            {
                name: 'listModels',
                description: 'List all available DataFlood models',
                inputSchema: {
                    type: 'object',
                    properties: {}
                }
            },
            {
                name: 'getModelInfo',
                description: 'Get detailed information about a model',
                inputSchema: {
                    type: 'object',
                    properties: {
                        model: { type: 'string', description: 'Model name' }
                    },
                    required: ['model']
                }
            }
        ];
        
        // Define available prompts
        this.prompts = [
            {
                name: 'create_ecommerce_db',
                description: 'Create a complete e-commerce database with products, customers, and orders'
            },
            {
                name: 'create_user_profile',
                description: 'Generate a user profile model with common fields'
            },
            {
                name: 'analyze_model',
                description: 'Analyze an existing model and provide insights'
            },
            {
                name: 'generation_control',
                description: 'Learn how to control document generation with $seed and $entropy parameters'
            }
        ];
        
        // Define available resources
        this.resources = [
            {
                uri: 'models://list',
                name: 'Available Models',
                description: 'List of all trained DataFlood models',
                mimeType: 'application/json'
            },
            {
                uri: 'servers://status',
                name: 'Server Status',
                description: 'Status of all MongoDB servers',
                mimeType: 'application/json'
            },
            {
                uri: 'models://{name}/schema',
                name: 'Model Schema',
                description: 'Get the JSON schema for a specific model',
                mimeType: 'application/json'
            },
            {
                uri: 'models://{name}/sample',
                name: 'Model Sample Data',
                description: 'Get sample data from a model',
                mimeType: 'application/json'
            },
            {
                uri: 'docs://generation-control',
                name: 'Generation Control Documentation',
                description: 'Documentation for $seed and $entropy query parameters',
                mimeType: 'text/markdown'
            }
        ];
        
        // Initialize transport
        this.initializeTransport();
    }
    
    /**
     * Initialize the transport mechanism
     */
    initializeTransport() {
        if (this.options.transport === 'stdio') {
            this.initializeStdio();
        } else if (this.options.transport === 'tcp') {
            this.initializeTcp();
        }
    }
    
    /**
     * Initialize stdio transport
     */
    initializeStdio() {
        const rl = readline.createInterface({
            input: process.stdin,
            output: null,  // Don't output anything
            terminal: false
        });
        
        rl.on('line', (line) => {
            if (line.trim()) {
                try {
                    const message = JSON.parse(line);
                    // Ignore error responses from client (not part of MCP spec)
                    if (message.error && !message.method) {
                        this.logger.debug('Ignoring error response from client:', message);
                        return;
                    }
                    this.handleMessage(message);
                } catch (err) {
                    // Log parse errors to file, never to stdout in stdio mode
                    this.logger.debug('Failed to parse message:', line);
                    // Send a proper JSON-RPC error if we can identify an ID
                    try {
                        const partial = JSON.parse(line);
                        if (partial.id) {
                            this.sendError(partial.id, -32700, 'Parse error', null);
                        }
                    } catch {
                        // Can't send error without ID, just log it
                        this.logger.debug('Cannot send error response - no message ID');
                    }
                }
            }
        });
        
        process.stdin.on('end', () => {
            this.shutdown();
        });
    }
    
    /**
     * Initialize TCP transport
     */
    initializeTcp() {
        this.tcpServer = net.createServer((socket) => {
            this.logger.info('MCP client connected');
            
            let buffer = '';
            
            socket.on('data', (data) => {
                buffer += data.toString();
                
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';
                
                for (const line of lines) {
                    if (line.trim()) {
                        try {
                            const message = JSON.parse(line);
                            this.handleMessage(message, socket);
                        } catch (err) {
                            this.logger.error('Failed to parse message:', err);
                        }
                    }
                }
            });
            
            socket.on('end', () => {
                this.logger.info('MCP client disconnected');
            });
        });
        
        this.tcpServer.listen(this.options.port, this.options.host, () => {
            this.logger.info(`MCP server listening on ${this.options.host}:${this.options.port}`);
        });
    }
    
    /**
     * Handle incoming JSON-RPC message
     */
    async handleMessage(message, socket = null) {
        this.logger.debug('Received message:', message);
        
        try {
            switch (message.method) {
                case 'initialize':
                    await this.handleInitialize(message, socket);
                    break;
                case 'initialized':
                case 'notifications/initialized':
                    // Client confirmation - no response needed
                    this.logger.debug('Client initialized');
                    break;
                case 'tools/list':
                    await this.handleListTools(message, socket);
                    break;
                case 'tools/call':
                    await this.handleToolCall(message, socket);
                    break;
                case 'prompts/list':
                    await this.handleListPrompts(message, socket);
                    break;
                case 'prompts/get':
                    await this.handleGetPrompt(message, socket);
                    break;
                case 'resources/list':
                    await this.handleListResources(message, socket);
                    break;
                case 'resources/read':
                    await this.handleReadResource(message, socket);
                    break;
                case 'models/list':
                    await this.handleModelsList(message, socket);
                    break;
                case 'models/query':
                    await this.handleModelsQuery(message, socket);
                    break;
                case 'completion/complete':
                    await this.handleCompletion(message, socket);
                    break;
                case 'logging/setLevel':
                    await this.handleSetLogLevel(message, socket);
                    break;
                case 'shutdown':
                    await this.handleShutdown(message, socket);
                    break;
                default:
                    this.sendError(message.id, -32601, `Method not found: ${message.method}`, socket);
            }
        } catch (err) {
            this.logger.error('Error handling message:', err);
            this.sendError(message.id, -32603, err.message, socket);
        }
    }
    
    /**
     * Handle initialize request
     */
    async handleInitialize(message, socket) {
        const response = {
            jsonrpc: '2.0',
            id: message.id,
            result: {
                protocolVersion: '2024-11-05',
                capabilities: {
                    tools: {},
                    prompts: {},
                    resources: {},
                    logging: {}
                },
                serverInfo: {
                    name: 'MongTap',
                    version: '0.2.0',
                    description: 'MongoDB-compatible database with DataFlood statistical modeling'
                }
            }
        };
        
        this.sendResponse(response, socket);
    }
    
    /**
     * Handle list tools request
     */
    async handleListTools(message, socket) {
        const response = {
            jsonrpc: '2.0',
            id: message.id,
            result: {
                tools: this.tools
            }
        };
        
        this.sendResponse(response, socket);
    }
    
    /**
     * Handle list prompts request
     */
    async handleListPrompts(message, socket) {
        const response = {
            jsonrpc: '2.0',
            id: message.id,
            result: {
                prompts: this.prompts
            }
        };
        
        this.sendResponse(response, socket);
    }
    
    /**
     * Handle get prompt request
     */
    async handleGetPrompt(message, socket) {
        const { name, arguments: args = {} } = message.params;
        
        const prompt = this.prompts.find(p => p.name === name);
        if (!prompt) {
            this.sendError(message.id, -32602, `Prompt not found: ${name}`, socket);
            return;
        }
        
        let messages = [];
        
        switch (name) {
            case 'create_ecommerce_db':
                messages = [
                    {
                        role: 'user',
                        content: {
                            type: 'text',
                            text: `Create an e-commerce database with ${args.num_products || 100} products and ${args.num_customers || 50} customers. Include orders that link them together.`
                        }
                    },
                    {
                        role: 'assistant',
                        content: {
                            type: 'text',
                            text: 'I\'ll create a complete e-commerce database for you. Let me set up the models and generate the data.'
                        }
                    }
                ];
                break;
                
            case 'create_user_profile':
                const fields = args.include_social 
                    ? 'name, email, age, bio, avatar, social media handles (twitter, linkedin, github)'
                    : 'name, email, age, bio, avatar';
                messages = [
                    {
                        role: 'user',
                        content: {
                            type: 'text',
                            text: `Create a user profile model with these fields: ${fields}`
                        }
                    }
                ];
                break;
                
            case 'analyze_model':
                messages = [
                    {
                        role: 'user',
                        content: {
                            type: 'text',
                            text: `Analyze the model "${args.model_name}" and provide insights about its structure, data distribution, and quality.`
                        }
                    }
                ];
                break;
                
            case 'generation_control':
                messages = [
                    {
                        role: 'user',
                        content: {
                            type: 'text',
                            text: 'How can I control the generation of documents from DataFlood models?'
                        }
                    },
                    {
                        role: 'assistant',
                        content: {
                            type: 'text',
                            text: `MongTap supports special query parameters to control document generation:

**Generation Control Parameters:**

1. **$seed** (or _seed): Provides deterministic generation
   - Same seed = same documents every time
   - Useful for testing and reproducible results
   - Example: { $seed: 12345, price: { $gt: 100 } }

2. **$entropy** (or _entropy): Controls randomness level (0.0 to 1.0)
   - 0.0 = Minimal variation (more predictable)
   - 1.0 = Maximum variation (more random)
   - Default is model-specific
   - Example: { $entropy: 0.3, category: "electronics" }

**Usage Examples:**

// Reproducible generation with seed
db.collection('stocks').find({ 
    $seed: 42,
    sector: 'Technology' 
}).limit(10)

// Low entropy for consistent data
db.collection('users').find({
    $entropy: 0.1,
    $seed: 100,
    age: { $gte: 18, $lte: 65 }
})

// Combine with regular MongoDB operators
db.collection('products').find({
    _seed: 999,
    _entropy: 0.5,
    price: { $between: [10, 100] },
    inStock: true
})

**Important Notes:**
- Generation parameters don't affect filtering
- They only control how synthetic documents are generated
- Work with all MongoDB query operators
- Both $ and _ prefixes are supported`
                        }
                    }
                ];
                break;
        }
        
        const response = {
            jsonrpc: '2.0',
            id: message.id,
            result: {
                description: prompt.description,
                messages
            }
        };
        
        this.sendResponse(response, socket);
    }
    
    /**
     * Handle list resources request
     */
    async handleListResources(message, socket) {
        const response = {
            jsonrpc: '2.0',
            id: message.id,
            result: {
                resources: this.resources
            }
        };
        
        this.sendResponse(response, socket);
    }
    
    /**
     * Handle read resource request
     */
    async handleReadResource(message, socket) {
        const { uri } = message.params;
        
        try {
            let contents = [];
            
            if (uri === 'models://list') {
                // List all models
                const models = await this.storage.listCollections(config.storage.defaultDatabase);
                contents = [{
                    uri,
                    mimeType: 'application/json',
                    text: JSON.stringify({
                        models: models.map(name => ({
                            name,
                            path: `models://${name}/schema`
                        }))
                    }, null, 2)
                }];
                
            } else if (uri === 'servers://status') {
                // Server status
                const servers = [];
                for (const [port, server] of this.servers) {
                    servers.push({
                        port,
                        database: server.options.database,
                        status: server.server.listening ? 'running' : 'stopped',
                        connections: server.connections ? server.connections.size : 0
                    });
                }
                contents = [{
                    uri,
                    mimeType: 'application/json',
                    text: JSON.stringify({ servers }, null, 2)
                }];
                
            } else if (uri.startsWith('models://') && uri.endsWith('/schema')) {
                // Model schema
                const modelName = uri.replace('models://', '').replace('/schema', '');
                const model = await this.storage.getModel(config.storage.defaultDatabase, modelName);
                if (!model) {
                    throw new Error(`Model not found: ${modelName}`);
                }
                contents = [{
                    uri,
                    mimeType: 'application/json',
                    text: JSON.stringify(model, null, 2)
                }];
                
            } else if (uri.startsWith('models://') && uri.endsWith('/sample')) {
                // Sample data from model
                const modelName = uri.replace('models://', '').replace('/sample', '');
                const model = await this.storage.getModel(config.storage.defaultDatabase, modelName);
                if (!model) {
                    throw new Error(`Model not found: ${modelName}`);
                }
                const samples = this.generator.generateDocuments(model, 5);
                contents = [{
                    uri,
                    mimeType: 'application/json',
                    text: JSON.stringify(samples, null, 2)
                }];
                
            } else if (uri === 'docs://generation-control') {
                // Generation control documentation
                contents = [{
                    uri,
                    mimeType: 'text/markdown',
                    text: `# Generation Control Parameters

MongTap supports special query parameters to control how DataFlood generates synthetic documents.

## Overview

When querying DataFlood-backed collections through MongoDB, you can use special parameters to control the generation process without affecting the query filtering.

## Parameters

### $seed (or _seed)
- **Type**: Number
- **Purpose**: Provides deterministic, reproducible generation
- **Behavior**: Same seed value always produces the same documents
- **Use Case**: Testing, debugging, reproducible demos

### $entropy (or _entropy)  
- **Type**: Number (0.0 to 1.0)
- **Purpose**: Controls the randomness/variation level
- **Values**:
  - 0.0 = Minimal variation (highly predictable)
  - 0.5 = Moderate variation (balanced)
  - 1.0 = Maximum variation (highly random)
- **Default**: Model-specific, typically around 0.5

## Examples

### Basic Usage
\`\`\`javascript
// Reproducible generation
db.collection('users').find({ 
    $seed: 12345 
}).limit(10)

// Low variation
db.collection('products').find({ 
    $entropy: 0.1 
}).limit(20)
\`\`\`

### Combined with Query Filters
\`\`\`javascript
// Seed + regular query
db.collection('stocks').find({
    $seed: 42,
    price: { $gt: 100 },
    sector: 'Technology'
})

// Entropy + seed + filters
db.collection('orders').find({
    _seed: 999,
    _entropy: 0.3,
    status: 'completed',
    total: { $gte: 50 }
})
\`\`\`

## Implementation Details

1. Parameters are extracted before query execution
2. They control the DocumentGenerator's random number generation
3. Parameters are removed from the query before filtering documents
4. Both $ and _ prefixes are supported for compatibility

## Benefits

- **Reproducible Testing**: Use seeds to create consistent test data
- **Controlled Variation**: Adjust entropy for realistic vs. predictable data
- **Seamless Integration**: Works with all MongoDB query operators
- **No Side Effects**: Generation params don't affect query filtering

## Technical Notes

- Implemented in: \`collection-manager.js\`
- Methods: \`extractGenerationParams()\`, \`removeGenerationParams()\`
- Generator: \`DocumentGenerator(seed, entropyOverride)\`
`
                }];
                
            } else {
                throw new Error(`Unknown resource: ${uri}`);
            }
            
            const response = {
                jsonrpc: '2.0',
                id: message.id,
                result: {
                    contents
                }
            };
            
            this.sendResponse(response, socket);
            
        } catch (err) {
            this.sendError(message.id, -32602, err.message, socket);
        }
    }
    
    /**
     * Handle tool call
     */
    async handleToolCall(message, socket) {
        const { name, arguments: args } = message.params;
        
        try {
            let result;
            
            switch (name) {
                case 'generateDataModel':
                    result = await this.generateDataModel(args);
                    break;
                case 'startMongoServer':
                    result = await this.startMongoServer(args);
                    break;
                case 'stopMongoServer':
                    result = await this.stopMongoServer(args);
                    break;
                case 'listActiveServers':
                    result = await this.listActiveServers();
                    break;
                case 'queryModel':
                    result = await this.queryModel(args);
                    break;
                case 'trainModel':
                    result = await this.trainModel(args);
                    break;
                case 'listModels':
                    result = await this.listModels();
                    break;
                case 'getModelInfo':
                    result = await this.getModelInfo(args);
                    break;
                case 'get_server_status':
                    // Get server status tool
                    const uptime = Math.floor((Date.now() - this.startTime) / 1000);
                    const models = await this.storage.listCollections(config.storage.defaultDatabase);
                    result = {
                        uptime,
                        models_loaded: models.length,
                        active_servers: this.servers.size,
                        transport: this.options.transport
                    };
                    break;
                default:
                    throw new Error(`Unknown tool: ${name}`);
            }
            
            const response = {
                jsonrpc: '2.0',
                id: message.id,
                result: {
                    content: [
                        {
                            type: 'text',
                            text: JSON.stringify(result, null, 2)
                        }
                    ]
                }
            };
            
            this.sendResponse(response, socket);
        } catch (err) {
            this.sendError(message.id, -32603, err.message, socket);
        }
    }
    
    /**
     * Handle completion request
     */
    /**
     * Handle models/list request
     */
    async handleModelsList(message, socket) {
        try {
            const models = await this.listModels();
            const response = {
                jsonrpc: '2.0',
                id: message.id,
                result: models
            };
            this.sendResponse(response, socket);
        } catch (err) {
            this.logger.error('Error listing models:', err);
            this.sendError(message.id, -32603, err.message, socket);
        }
    }
    
    /**
     * Handle models/query request
     */
    async handleModelsQuery(message, socket) {
        try {
            const { model, count = 10, constraints } = message.params;
            const result = await this.queryModel({ model, count, constraints });
            const response = {
                jsonrpc: '2.0',
                id: message.id,
                result
            };
            this.sendResponse(response, socket);
        } catch (err) {
            this.logger.error('Error querying model:', err);
            this.sendError(message.id, -32603, err.message, socket);
        }
    }
    
    async handleCompletion(message, socket) {
        const { ref, argument, value } = message.params;
        
        const response = {
            jsonrpc: '2.0',
            id: message.id,
            result: {
                completion: {
                    values: []
                }
            }
        };
        
        // Add completions based on context
        if (ref && ref.type === 'ref/prompt' && argument === 'model_name') {
            // Suggest available models
            const models = await this.storage.listCollections(config.storage.defaultDatabase);
            response.result.completion.values = models.map(name => ({ value: name }));
        }
        
        this.sendResponse(response, socket);
    }
    
    /**
     * Handle set log level
     */
    async handleSetLogLevel(message, socket) {
        const { level } = message.params;
        // Update log level if needed
        this.logger.info(`Log level set to: ${level}`);
        
        const response = {
            jsonrpc: '2.0',
            id: message.id,
            result: {}
        };
        
        this.sendResponse(response, socket);
    }
    
    /**
     * Handle shutdown request
     */
    async handleShutdown(message, socket) {
        await this.shutdown();
        
        const response = {
            jsonrpc: '2.0',
            id: message.id,
            result: {}
        };
        
        this.sendResponse(response, socket);
    }
    
    // Tool implementations (same as before)
    async generateDataModel(args) {
        const { name, description, samples } = args;
        
        if (samples && samples.length > 0) {
            const model = this.inferrer.inferSchema(samples);
            model.title = name;
            model.description = description || `DataFlood model: ${name}`;
            
            await this.storage.saveModel(config.storage.defaultDatabase, name, model);
            this.models.set(name, model);
            
            return {
                success: true,
                message: `Model '${name}' created from ${samples.length} samples`,
                properties: Object.keys(model.properties || {})
            };
        } else if (description) {
            const model = this.generateFromDescription(description);
            model.title = name;
            
            await this.storage.saveModel(config.storage.defaultDatabase, name, model);
            this.models.set(name, model);
            
            return {
                success: true,
                message: `Model '${name}' generated from description`,
                properties: Object.keys(model.properties || {})
            };
        } else {
            throw new Error('Either samples or description required');
        }
    }
    
    async startMongoServer(args) {
        const { port = 0, database = 'test' } = args;
        
        const server = new MongoDBServer({
            port,
            storage: this.storage,
            database,
            logger: this.logger  // Pass the MCP logger
        });
        
        await server.start();
        const actualPort = server.server.address().port;
        
        this.servers.set(actualPort, server);
        
        return {
            success: true,
            port: actualPort,
            message: `MongoDB server started on port ${actualPort}`,
            connectionString: `mongodb://localhost:${actualPort}/${database}`
        };
    }
    
    async stopMongoServer(args) {
        const { port } = args;
        
        const server = this.servers.get(port);
        if (!server) {
            throw new Error(`No server running on port ${port}`);
        }
        
        try {
            // Stop the server and wait for it to fully close
            await server.stop();
            
            // Verify the server is actually stopped
            const isListening = server.server && server.server.listening;
            if (isListening) {
                this.logger.warn(`Server on port ${port} may not have stopped completely`);
            }
            
        } catch (error) {
            this.logger.error(`Error stopping server on port ${port}:`, error);
            throw error;
        } finally {
            // Always remove from map, even if stop failed
            this.servers.delete(port);
        }
        
        return {
            success: true,
            message: `Server on port ${port} stopped`
        };
    }
    
    async listActiveServers() {
        const servers = [];
        const stoppedServers = [];
        
        for (const [port, server] of this.servers) {
            if (server.server && server.server.listening) {
                servers.push({
                    port,
                    status: 'running',
                    connections: server.connections ? server.connections.size : 0
                });
            } else {
                // Server is stopped but still in map - mark for cleanup
                stoppedServers.push(port);
            }
        }
        
        // Clean up stopped servers from the map
        for (const port of stoppedServers) {
            this.logger.debug(`Cleaning up stopped server on port ${port}`);
            this.servers.delete(port);
        }
        
        return {
            count: servers.length,
            servers
        };
    }
    
    async queryModel(args) {
        const { model, query = {}, count = 10 } = args;
        
        if (!this.models.has(model)) {
            const loaded = await this.storage.getModel(config.storage.defaultDatabase, model);
            if (!loaded) {
                throw new Error(`Model '${model}' not found`);
            }
            this.models.set(model, loaded);
        }
        
        const schema = this.models.get(model);
        const documents = this.generator.generateDocuments(schema, count);
        
        return {
            model,
            count: documents.length,
            documents
        };
    }
    
    async trainModel(args) {
        const { model, documents } = args;
        
        if (!documents || documents.length === 0) {
            throw new Error('No documents provided');
        }
        
        const trained = await this.storage.trainModel(config.storage.defaultDatabase, model, documents);
        this.models.set(model, trained);
        
        return {
            success: true,
            message: `Model '${model}' trained with ${documents.length} documents`,
            properties: Object.keys(trained.properties || {})
        };
    }
    
    async listModels() {
        const models = [];
        const collections = await this.storage.listCollections(config.storage.defaultDatabase);
        
        for (const name of collections) {
            try {
                const model = await this.storage.getModel(config.storage.defaultDatabase, name);
                if (model) {
                    models.push({
                        name,
                        properties: Object.keys(model.properties || {}),
                        description: model.description || `DataFlood model for ${name}`
                    });
                } else {
                    // Add model even if we can't load it fully
                    models.push({
                        name,
                        properties: [],
                        description: `Model ${name} (loading error)`
                    });
                }
            } catch (err) {
                this.logger.warn(`Error loading model ${name}:`, err.message);
                // Still add the model to the list
                models.push({
                    name,
                    properties: [],
                    description: `Model ${name} available`
                });
            }
        }
        
        return {
            count: models.length,
            models
        };
    }
    
    async getModelInfo(args) {
        const { model } = args;
        
        const schema = await this.storage.getModel(config.storage.defaultDatabase, model);
        if (!schema) {
            throw new Error(`Model '${model}' not found`);
        }
        
        return {
            name: model,
            title: schema.title,
            description: schema.description,
            properties: schema.properties,
            required: schema.required || []
        };
    }
    
    generateFromDescription(description) {
        const analysis = this.promptAnalyzer.analyze(description);
        return analysis.schema;
    }
    
    /**
     * Send response
     */
    sendResponse(response, socket) {
        const message = JSON.stringify(response) + '\n';
        
        if (socket) {
            socket.write(message);
        } else if (this.options.transport === 'stdio') {
            process.stdout.write(message);
        }
    }
    
    /**
     * Send error response
     */
    sendError(id, code, message, socket) {
        const response = {
            jsonrpc: '2.0',
            id,
            error: {
                code,
                message
            }
        };
        
        this.sendResponse(response, socket);
    }
    
    /**
     * Send notification
     */
    sendNotification(method, params, socket) {
        const notification = {
            jsonrpc: '2.0',
            method,
            params
        };
        
        this.sendResponse(notification, socket);
    }
    
    /**
     * Shutdown server
     */
    async shutdown() {
        this.logger.info('Shutting down MCP server...');
        
        // Stop all MongoDB servers
        for (const [port, server] of this.servers) {
            await server.stop();
        }
        
        // Close TCP server if running
        if (this.tcpServer) {
            this.tcpServer.close();
        }
        
        this.emit('shutdown');
    }
}

export default MCPServerEnhanced;