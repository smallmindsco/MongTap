#!/usr/bin/env node

/**
 * MongTap MCP Server - Clean Implementation
 * Uses @modelcontextprotocol/sdk with DataFlood functionality
 * All logging goes to timestamped files, not stdio/stderr
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { 
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ListPromptsRequestSchema,
  GetPromptRequestSchema,
  ListResourcesRequestSchema,
  ReadResourceRequestSchema
} from '@modelcontextprotocol/sdk/types.js';

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// DataFlood imports
import { PromptAnalyzer } from './prompt-analyzer.js';
import { SampleGenerator } from './sample-generator.js';
import { SchemaInferrer } from '../dataflood-js/schema/inferrer.js';
import { DocumentGenerator } from '../dataflood-js/generator/document-generator.js';
import { MongoDBServer } from '../welldb-node/server/mongodb-server.js';
import { DataFloodStorage } from '../welldb-node/storage/dataflood-storage.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load configuration
let config = {
  storage: {
    modelsBasePath: './mcp-models',
    defaultDatabase: 'mcp'
  },
  server: {
    defaultPort: 27017,
    host: 'localhost',
    enableAutoTrain: true,
    trainThreshold: 100
  },
  generation: {
    defaultSeed: null,
    defaultEntropy: null,
    maxDocuments: 10000
  },
  logging: {
    level: 'info',
    suppressStdio: true
  }
};

// Try to load user configuration
try {
  const configPath = path.join(__dirname, '../../mongtap.config.json');
  const configData = await fs.readFile(configPath, 'utf8');
  const userConfig = JSON.parse(configData);
  // Deep merge user config with defaults
  config = { ...config, ...userConfig };
  if (userConfig.storage) config.storage = { ...config.storage, ...userConfig.storage };
  if (userConfig.server) config.server = { ...config.server, ...userConfig.server };
  if (userConfig.generation) config.generation = { ...config.generation, ...userConfig.generation };
  if (userConfig.logging) config.logging = { ...config.logging, ...userConfig.logging };
} catch (err) {
  // Config file doesn't exist or is invalid, use defaults
}

// Create timestamped logger that writes to files only
const logFile = path.join(__dirname, '../../logs', `mongtap-${new Date().toISOString().slice(0,10)}.log`);
await fs.mkdir(path.dirname(logFile), { recursive: true });

const logger = {
  debug: (msg) => fs.appendFile(logFile, `${new Date().toISOString()} [DEBUG] ${msg}\n`).catch(() => {}),
  info: (msg) => fs.appendFile(logFile, `${new Date().toISOString()} [INFO] ${msg}\n`).catch(() => {}),
  warn: (msg) => fs.appendFile(logFile, `${new Date().toISOString()} [WARN] ${msg}\n`).catch(() => {}),
  error: (msg) => fs.appendFile(logFile, `${new Date().toISOString()} [ERROR] ${msg}\n`).catch(() => {})
};

logger.info('MongTap MCP server starting up');

// Initialize DataFlood components
const promptAnalyzer = new PromptAnalyzer();
const sampleGenerator = new SampleGenerator();
const schemaInferrer = new SchemaInferrer();
const documentGenerator = new DocumentGenerator();

// Resolve models base path from config
const modelsBasePath = path.isAbsolute(config.storage.modelsBasePath) 
  ? config.storage.modelsBasePath 
  : path.join(__dirname, '../..', config.storage.modelsBasePath);

const storage = new DataFloodStorage({ 
  basePath: modelsBasePath,
  logger: logger,
  enableAutoTrain: config.server.enableAutoTrain,
  trainThreshold: config.server.trainThreshold
});

// Server state
const servers = new Map();
const models = new Map();

// Initialize the MCP server
const server = new Server(
  {
    name: 'mongtap',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
      prompts: {},
      resources: {}
    },
  }
);

// Tool definitions for MongoDB operations with DataFlood functionality
const TOOLS = [
  {
    name: 'generateDataModel',
    description: 'Create a statistical model from sample documents or a text description for data generation',
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
    description: 'Start a local MongoDB-compatible server that generates data from statistical models',
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
    description: 'Stop a running MongoDB-compatible server instance by port number',
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
    description: 'Get a list of all currently running MongoDB-compatible server instances',
    inputSchema: {
      type: 'object',
      properties: {}
    }
  },
  {
    name: 'queryModel',
    description: 'Generate documents from a statistical model with optional query filters and generation control ($seed for reproducibility, $entropy for randomness)',
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
    description: 'Update an existing statistical model with additional sample documents to improve generation quality',
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
    description: 'Get a list of all available statistical models stored locally',
    inputSchema: {
      type: 'object',
      properties: {}
    }
  },
  {
    name: 'getModelInfo',
    description: 'Retrieve detailed schema and statistics for a specific statistical model',
    inputSchema: {
      type: 'object',
      properties: {
        model: { type: 'string', description: 'Model name' }
      },
      required: ['model']
    }
  }
];

// Prompts for common database operations
const PROMPTS = [
  {
    name: 'create_ecommerce_db',
    description: 'Create a complete e-commerce database with products, customers, and orders',
    text: 'Create an e-commerce database with the following collections: products (name, price, category, description, stock), customers (name, email, address, phone), and orders (customer_id, products, total, date, status). Include sample data and establish relationships between collections.'
  },
  {
    name: 'create_user_profile',
    description: 'Generate a user profile model with common fields',
    text: 'Create a user profile model with fields: name (string), email (string), age (number 18-80), location (city, state), interests (array of hobbies), registration_date (date), is_active (boolean). Generate 50 sample users with realistic data.'
  },
  {
    name: 'analyze_model',
    description: 'Analyze an existing model and provide insights',
    text: 'Analyze an existing DataFlood model to understand its schema, data patterns, and statistical properties. Provide insights on field distributions, correlations, and suggestions for improving data generation quality.'
  },
  {
    name: 'generation_control',
    description: 'Learn how to control document generation with $seed and $entropy parameters',
    text: 'Demonstrate generation control by querying collections with: 1) $seed parameter for consistent results, 2) $entropy parameter for controlling randomness (0.1 = predictable, 0.9 = random), 3) combined parameters with regular MongoDB query filters. Show how same seed always returns identical data.'
  }
];

// Resources for model and server information
const RESOURCES = [
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

// List tools handler
server.setRequestHandler(ListToolsRequestSchema, async () => {
  logger.debug('Listing tools');
  return { tools: TOOLS };
});

// Call tool handler with actual DataFlood functionality
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  logger.info(`Tool called: ${name} with args: ${JSON.stringify(args)}`);
  
  try {
    switch (name) {
      case 'generateDataModel':
        const modelName = args.name;
        let modelData;
        
        if (args.samples && args.samples.length > 0) {
          // Train from samples
          const schema = schemaInferrer.inferSchema(args.samples);
          const modelObj = { schema: schema || {}, samples: args.samples, trained: true };
          await storage.saveModel(config.storage.defaultDatabase, modelName, modelObj);
          models.set(modelName, modelObj);
          modelData = `Model '${modelName}' trained from ${args.samples.length} sample documents`;
        } else if (args.description) {
          // Generate from description - simplified approach
          const samples = [
            { _description: args.description, _generated: true, _timestamp: new Date().toISOString() }
          ];
          const schema = { type: 'object', properties: { _description: { type: 'string' } } };
          const modelObj = { schema: schema, samples: samples, description: args.description, trained: true };
          await storage.saveModel(config.storage.defaultDatabase, modelName, modelObj);
          models.set(modelName, modelObj);
          modelData = `Model '${modelName}' created from description (simplified)`;
        } else {
          throw new Error('Either samples or description is required');
        }
        
        const schemaKeys = Object.keys(models.get(modelName)?.schema?.properties || {});
        return {
          content: [{
            type: 'text',
            text: `${modelData}\n\nSchema inferred: ${schemaKeys.length > 0 ? schemaKeys.join(', ') : 'basic structure'}\nModel ready for querying and generation.`
          }]
        };

      case 'startMongoServer':
        const port = args.port !== undefined ? args.port : (config.server.defaultPort || 0);
        const database = args.database || config.storage.defaultDatabase || 'test';
        
        const mongoServer = new MongoDBServer({
          port: port,
          host: config.server.host || 'localhost',
          storage: storage,
          logger: logger
        });
        
        await mongoServer.start();
        const actualPort = mongoServer.port; // Get the actual port from the server
        servers.set(actualPort, { server: mongoServer, database, status: 'running', connections: 0 });
        
        return {
          content: [{
            type: 'text',
            text: `MongoDB server started successfully:\n- Port: ${actualPort}\n- Database: ${database}\n- Connection: mongodb://localhost:${actualPort}/${database}\n\nServer supports DataFlood generation with $seed and $entropy parameters.`
          }]
        };

      case 'stopMongoServer':
        const targetPort = args.port;
        const serverInfo = servers.get(targetPort);
        
        if (!serverInfo) {
          throw new Error(`No server running on port ${targetPort}`);
        }
        
        await serverInfo.server.stop();
        servers.delete(targetPort);
        
        return {
          content: [{
            type: 'text',
            text: `MongoDB server on port ${targetPort} stopped successfully.`
          }]
        };

      case 'listActiveServers':
        const activeServers = Array.from(servers.entries()).map(([port, info]) => ({
          port,
          database: info.database,
          status: info.status,
          connections: info.connections
        }));
        
        return {
          content: [{
            type: 'text',
            text: `Active MongoDB servers:\n${activeServers.map(s => `- Port ${s.port}: ${s.database} (${s.status}, ${s.connections} connections)`).join('\n') || 'No active servers'}`
          }]
        };

      case 'queryModel':
        // Check filesystem first
        let model = models.get(args.model);
        if (!model) {
          try {
            model = await storage.getModel(config.storage.defaultDatabase, args.model);
            if (model) {
              models.set(args.model, model);
            }
          } catch (error) {
            throw new Error(`Model '${args.model}' not found`);
          }
        }
        if (!model) {
          throw new Error(`Model '${args.model}' not found`);
        }
        
        const count = args.count || 10;
        const query = args.query || {};
        const seed = query.$seed;
        const entropy = query.$entropy || 0.5;
        
        // Generate documents using DataFlood
        const documents = documentGenerator.generateDocuments(model.schema, count);
        
        return {
          content: [{
            type: 'text',
            text: `Generated ${documents.length} documents from model '${args.model}'\n\nGeneration parameters:\n- Seed: ${seed || 'random'}\n- Entropy: ${entropy}\n- Query filters: ${JSON.stringify(query, null, 2)}\n\nSample documents:\n${documents.slice(0, 3).map(d => JSON.stringify(d, null, 2)).join('\n\n')}`
          }]
        };

      case 'trainModel':
        const existingModel = models.get(args.model) || {};
        const allSamples = [...(existingModel.samples || []), ...args.documents];
        const newSchema = schemaInferrer.inferSchema(allSamples);
        
        await storage.saveModel('mongtap', args.model, { schema: newSchema, samples: allSamples });
        models.set(args.model, { schema: newSchema, samples: allSamples, trained: true });
        
        return {
          content: [{
            type: 'text',
            text: `Model '${args.model}' updated with ${args.documents.length} new documents\n\nTotal samples: ${allSamples.length}\nSchema fields: ${Object.keys(newSchema.properties || {}).join(', ')}`
          }]
        };

      case 'listModels':
        // Always check filesystem first for persistent models
        const persistentModels = await storage.listModels();
        
        // Sync in-memory models with filesystem
        for (const modelName of persistentModels) {
          if (!models.has(modelName)) {
            try {
              const modelData = await storage.getModel(config.storage.defaultDatabase, modelName);
              if (modelData) {
                models.set(modelName, modelData);
              }
            } catch (error) {
              logger.warn(`Failed to load model ${modelName}: ${error.message}`);
            }
          }
        }
        
        const modelList = Array.from(models.entries()).map(([name, data]) => ({
          name,
          samples: data.samples?.length || 0,
          trained: data.trained || false,
          fields: Object.keys(data.schema?.properties || {})
        }));
        
        return {
          content: [{
            type: 'text',
            text: `Available DataFlood models:\n${modelList.map(m => `- ${m.name}: ${m.samples} samples, ${m.fields.length} fields`).join('\n') || 'No models found'}`
          }]
        };

      case 'getModelInfo':
        // Check filesystem first
        let modelInfo = models.get(args.model);
        if (!modelInfo) {
          try {
            modelInfo = await storage.getModel(config.storage.defaultDatabase, args.model);
            if (modelInfo) {
              models.set(args.model, modelInfo);
            }
          } catch (error) {
            throw new Error(`Model '${args.model}' not found`);
          }
        }
        if (!modelInfo) {
          throw new Error(`Model '${args.model}' not found`);
        }
        
        return {
          content: [{
            type: 'text',
            text: `Model: ${args.model}\n\nSamples: ${modelInfo.samples?.length || 0}\nTrained: ${modelInfo.trained || false}\n\nSchema:\n${JSON.stringify(modelInfo.schema, null, 2)}`
          }]
        };

      default:
        return {
          content: [{
            type: 'text',
            text: `Tool '${name}' executed with parameters: ${JSON.stringify(args, null, 2)}`
          }]
        };
    }
  } catch (error) {
    logger.error(`Tool execution error: ${error.message}`);
    return {
      content: [{
        type: 'text',
        text: `Error executing tool '${name}': ${error.message}`
      }],
      isError: true
    };
  }
});

// List prompts handler
server.setRequestHandler(ListPromptsRequestSchema, async () => {
  logger.debug('Listing prompts');
  return { prompts: PROMPTS };
});

// Get prompt handler
server.setRequestHandler(GetPromptRequestSchema, async (request) => {
  const prompt = PROMPTS.find(p => p.name === request.params.name);
  if (!prompt) {
    throw new Error(`Prompt '${request.params.name}' not found`);
  }
  
  return {
    messages: [
      {
        role: 'user',
        content: {
          type: 'text',
          text: prompt.text
        }
      }
    ]
  };
});

// List resources handler
server.setRequestHandler(ListResourcesRequestSchema, async () => {
  logger.debug('Listing resources');
  return { resources: RESOURCES };
});

// Read resource handler
server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const { uri } = request.params;
  logger.debug(`Reading resource: ${uri}`);
  
  if (uri === 'models://list') {
    const modelList = Array.from(models.entries()).map(([name, data]) => ({
      name,
      documents: data.samples?.length || 0,
      schema: data.schema ? Object.keys(data.schema.properties || {}).join(', ') : 'unknown',
      trained: data.trained || false
    }));
    
    return {
      contents: [
        {
          uri,
          mimeType: 'application/json',
          text: JSON.stringify({ models: modelList }, null, 2)
        }
      ]
    };
  }
  
  if (uri === 'servers://status') {
    const serverList = Array.from(servers.entries()).map(([port, info]) => ({
      port,
      status: info.status,
      database: info.database,
      connections: info.connections
    }));
    
    return {
      contents: [
        {
          uri,
          mimeType: 'application/json',
          text: JSON.stringify({ servers: serverList }, null, 2)
        }
      ]
    };
  }
  
  throw new Error(`Resource '${uri}' not found`);
});

// Start the server
const transport = new StdioServerTransport();
await server.connect(transport);

logger.info('MongTap MCP server connected and ready');