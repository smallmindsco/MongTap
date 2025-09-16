/**
 * DataFlood Storage Manager
 * Manages DataFlood models for MongoDB collections
 * Similar to WellDB's ModelRepository
 */

import { promises as fs, readdirSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import path from 'path';
const { join } = path;
import { DocumentGenerator } from '../../dataflood-js/generator/document-generator.js';
import { IncrementalTrainer } from '../../dataflood-js/training/incremental-trainer.js';
import { SchemaInferrer } from '../../dataflood-js/schema/inferrer.js';
import config from '../../config/config-loader.js';

/**
 * Storage manager for DataFlood models
 * Handles model persistence, loading, and caching
 */
export class DataFloodStorage {
    constructor(options = {}) {
        this.basePath = options.basePath || config.storage.modelsBasePath;
        this.modelCache = new Map();
        this.logger = options.logger || this.createDefaultLogger();
        this.maxCacheSize = options.maxCacheSize || 100;
        this.enableAutoTrain = options.enableAutoTrain !== false;
        this.trainThreshold = options.trainThreshold || config.server.trainThreshold;
        this.trainer = new IncrementalTrainer();
        this.generator = new DocumentGenerator();
        this.defaultDatabase = config.storage.defaultDatabase || 'mcp';
    }

    createDefaultLogger() {
        return {
            debug: () => {},
            info: () => {},  // Suppress info logs in stdio mode
            warn: () => {},   // Suppress warnings in stdio mode
            error: () => {}   // Suppress errors - should never output to stderr
        };
    }

    /**
     * Get cache key for database and collection
     */
    getCacheKey(database, collection) {
        return `${database}:${collection}`;
    }

    /**
     * Get file path for a model
     */
    getModelPath(database, collection) {
        return join(this.basePath, database, `${collection}.json`);
    }

    /**
     * Load a model from disk or cache
     */
    async getModel(database, collection) {
        const cacheKey = this.getCacheKey(database, collection);
        
        // Check cache first
        if (this.modelCache.has(cacheKey)) {
            this.logger.debug(`Model cache hit for ${cacheKey}`);
            return this.modelCache.get(cacheKey);
        }

        // Try to load from disk
        const modelPath = this.getModelPath(database, collection);
        try {
            const data = await fs.readFile(modelPath, 'utf8');
            const model = JSON.parse(data);
            
            // Add to cache
            this.addToCache(cacheKey, model);
            
            this.logger.info(`Loaded model for ${cacheKey} from ${modelPath}`);
            return model;
        } catch (err) {
            if (err.code === 'ENOENT') {
                this.logger.debug(`No model found for ${cacheKey}`);
                return null;
            }
            this.logger.error(`Error loading model for ${cacheKey}:`, err);
            throw err;
        }
    }

    /**
     * Save a model to disk and cache
     */
    async saveModel(database, collection, model) {
        const cacheKey = this.getCacheKey(database, collection);
        const modelPath = this.getModelPath(database, collection);
        
        // Ensure directory exists
        const dir = path.dirname(modelPath);
        await fs.mkdir(dir, { recursive: true });
        
        // Save to disk
        const data = JSON.stringify(model, null, 2);
        await fs.writeFile(modelPath, data, 'utf8');
        
        // Update cache
        this.addToCache(cacheKey, model);
        
        this.logger.info(`Saved model for ${cacheKey} to ${modelPath}`);
    }

    /**
     * Delete a model from disk and cache
     */
    async deleteModel(database, collection) {
        const cacheKey = this.getCacheKey(database, collection);
        const modelPath = this.getModelPath(database, collection);
        
        // Remove from cache
        this.modelCache.delete(cacheKey);
        
        // Delete from disk
        try {
            await fs.unlink(modelPath);
            this.logger.info(`Deleted model for ${cacheKey}`);
        } catch (err) {
            if (err.code !== 'ENOENT') {
                throw err;
            }
        }
    }

    /**
     * Create or update a model from inserted documents (for database collections)
     */
    async trainCollectionModel(database, collection, documents) {
        if (!this.enableAutoTrain) {
            return null;
        }

        const existingModel = await this.getModel(database, collection);
        
        // Use SchemaInferrer if no existing model
        let updatedModel;
        if (!existingModel) {
            const inferrer = new SchemaInferrer();
            updatedModel = inferrer.inferSchema(documents);
        } else {
            // Train incrementally
            updatedModel = this.trainer.train(documents, existingModel);
        }
        
        // Add collection metadata
        if (!updatedModel.title) {
            updatedModel.title = collection;
        }
        if (!updatedModel.description) {
            updatedModel.description = `DataFlood model for ${database}.${collection}`;
        }
        
        // Save the updated model
        await this.saveModel(database, collection, updatedModel);
        
        this.logger.info(`Trained model for ${database}.${collection} with ${documents.length} documents`);
        return updatedModel;
    }

    /**
     * Generate documents from a model
     */
    async generateDocuments(database, collection, count, options = {}) {
        const model = await this.getModel(database, collection);
        if (!model) {
            // No model exists, generate simple random documents
            return this.generateDefaultDocuments(count);
        }

        const seed = options.seed || null;
        const entropyOverride = options.entropyOverride || options.entropy || null;
        const constraints = options.constraints || {};
        
        // Generate with constraints if provided
        const generator = new DocumentGenerator(seed, entropyOverride);
        
        // Generate documents
        const generatedDocs = generator.generateDocuments(model, count);
        const documents = [];
        
        for (const doc of generatedDocs) {
            // Apply constraints
            for (const [field, constraint] of Object.entries(constraints)) {
                const currentValue = this.getNestedProperty(doc, field);
                const constrainedValue = this.applyConstraint(currentValue, constraint, field, model);
                this.setNestedProperty(doc, field, constrainedValue);
            }
            
            // Add MongoDB _id if not present
            if (!doc._id) {
                doc._id = this.generateObjectId();
            }
            
            documents.push(doc);
        }
        
        this.logger.debug(`Generated ${count} documents for ${database}.${collection}`);
        return documents;
    }
    
    /**
     * Apply constraint to a value
     */
    applyConstraint(currentValue, constraint, field, model) {
        // If constraint is a simple value (old format), return it
        if (typeof constraint !== 'object' || constraint === null) {
            return constraint;
        }
        
        // Handle constraint object with operators
        if (constraint.equals !== undefined) {
            return constraint.equals;
        }
        
        // For numeric fields, apply min/max constraints
        if (typeof currentValue === 'number') {
            let value = currentValue;
            
            if (constraint.min !== undefined) {
                const min = constraint.excludeMin ? constraint.min + 0.01 : constraint.min;
                value = Math.max(value, min);
            }
            
            if (constraint.max !== undefined) {
                const max = constraint.excludeMax ? constraint.max - 0.01 : constraint.max;
                value = Math.min(value, max);
            }
            
            // If value hasn't changed and we have min/max, generate new value in range
            if (value === currentValue && (constraint.min !== undefined || constraint.max !== undefined)) {
                const min = constraint.min || 0;
                const max = constraint.max || min + 100;
                value = min + Math.random() * (max - min);
                
                // Apply exclusion rules
                if (constraint.excludeMin && value <= constraint.min) {
                    value = constraint.min + 0.01;
                }
                if (constraint.excludeMax && value >= constraint.max) {
                    value = constraint.max - 0.01;
                }
            }
            
            return value;
        }
        
        // For enum constraint, pick a value from the list
        if (constraint.enum && Array.isArray(constraint.enum)) {
            return constraint.enum[Math.floor(Math.random() * constraint.enum.length)];
        }
        
        // Default: return current value
        return currentValue;
    }
    
    /**
     * Get nested property value
     */
    getNestedProperty(obj, path) {
        const parts = path.split('.');
        let current = obj;
        
        for (const part of parts) {
            if (current === null || current === undefined) {
                return undefined;
            }
            current = current[part];
        }
        
        return current;
    }

    /**
     * List all available models
     */
    async listModels() {
        const models = [];
        
        // Check default database directory
        const mcpDir = join(this.basePath, this.defaultDatabase);
        if (existsSync(mcpDir)) {
            const files = readdirSync(mcpDir);
            for (const file of files) {
                if (file.endsWith('.json')) {
                    models.push(file.replace('.json', ''));
                }
            }
        }
        
        // Check trained directory
        const trainedDir = join(this.basePath, 'trained');
        if (existsSync(trainedDir)) {
            const files = readdirSync(trainedDir);
            for (const file of files) {
                if (file.endsWith('.json')) {
                    const modelName = file.replace('.json', '');
                    if (!models.includes(modelName)) {
                        models.push(modelName);
                    }
                }
            }
        }
        
        return models;
    }

    /**
     * Load a model by name from mcp or trained directories
     */
    async loadModel(modelName) {
        // Check cache first
        const cacheKey = `mcp:${modelName}`;
        if (this.modelCache.has(cacheKey)) {
            return this.modelCache.get(cacheKey);
        }

        // Try loading from default database directory first
        const mcpPath = join(this.basePath, this.defaultDatabase, `${modelName}.json`);
        if (existsSync(mcpPath)) {
            try {
                const content = readFileSync(mcpPath, 'utf8');
                const model = JSON.parse(content);
                this.addToCache(cacheKey, model);
                return model;
            } catch (err) {
                this.logger.error(`Failed to load model from ${mcpPath}:`, err);
            }
        }

        // Try loading from trained directory
        const trainedPath = join(this.basePath, 'trained', `${modelName}.json`);
        if (existsSync(trainedPath)) {
            try {
                const content = readFileSync(trainedPath, 'utf8');
                const model = JSON.parse(content);
                this.addToCache(cacheKey, model);
                return model;
            } catch (err) {
                this.logger.error(`Failed to load model from ${trainedPath}:`, err);
            }
        }

        // Model not found
        return null;
    }

    /**
     * Generate a document from a model
     */
    async generateDocument(modelName, query = {}) {
        const model = await this.loadModel(modelName);
        if (!model) {
            throw new Error(`Model '${modelName}' not found`);
        }

        // Extract generation parameters from query
        const seed = query.$seed || query._seed || null;
        const entropyOverride = query.$entropy || query._entropy || null;
        
        const generator = new DocumentGenerator(seed, entropyOverride);
        
        // Generate document based on model
        let document = generator.generateDocument(model);
        
        // Apply query constraints if provided
        if (query && Object.keys(query).length > 0) {
            // Apply filters to the generated document
            for (const [key, value] of Object.entries(query)) {
                if (document.hasOwnProperty(key)) {
                    document[key] = value;
                }
            }
        }
        
        // Add MongoDB _id if not present
        if (!document._id) {
            document._id = this.generateObjectId();
        }
        
        return document;
    }

    /**
     * Train a model with new data
     */
    async trainModel(modelName, data) {
        const existingModel = await this.loadModel(modelName);
        
        let model;
        if (existingModel) {
            // Update existing model
            const trainer = new IncrementalTrainer();
            model = trainer.updateModel(existingModel, data);
        } else {
            // Create new model
            const inferrer = new SchemaInferrer();
            model = inferrer.inferSchema(data);
        }
        
        // Save the model
        const modelPath = join(this.basePath, this.defaultDatabase, `${modelName}.json`);
        const dir = path.dirname(modelPath);
        
        // Ensure directory exists
        if (!existsSync(dir)) {
            mkdirSync(dir, { recursive: true });
        }
        
        // Save to disk
        writeFileSync(modelPath, JSON.stringify(model, null, 2), 'utf8');
        
        // Update cache
        const cacheKey = `mcp:${modelName}`;
        this.addToCache(cacheKey, model);
        
        this.logger.info(`Trained model '${modelName}' with ${data.length} samples`);
        return model;
    }

    /**
     * Generate default documents when no model exists
     */
    generateDefaultDocuments(count) {
        const documents = [];
        for (let i = 0; i < count; i++) {
            documents.push({
                _id: this.generateObjectId(),
                index: i,
                timestamp: new Date(),
                random: Math.random()
            });
        }
        return documents;
    }

    /**
     * Generate a MongoDB ObjectId-like string
     */
    generateObjectId() {
        const timestamp = Math.floor(Date.now() / 1000).toString(16);
        const random = Math.random().toString(16).substring(2, 18);
        return (timestamp + random).substring(0, 24).padEnd(24, '0');
    }

    /**
     * Set a nested property in an object
     */
    setNestedProperty(obj, path, value) {
        const parts = path.split('.');
        let current = obj;
        
        for (let i = 0; i < parts.length - 1; i++) {
            const part = parts[i];
            if (!(part in current)) {
                current[part] = {};
            }
            current = current[part];
        }
        
        current[parts[parts.length - 1]] = value;
    }

    /**
     * Add model to cache with LRU eviction
     */
    addToCache(key, model) {
        // Remove if already exists (to update position)
        this.modelCache.delete(key);
        
        // Check cache size
        if (this.modelCache.size >= this.maxCacheSize) {
            // Remove oldest (first) entry
            const firstKey = this.modelCache.keys().next().value;
            this.modelCache.delete(firstKey);
            this.logger.debug(`Evicted ${firstKey} from cache`);
        }
        
        // Add to end (most recent)
        this.modelCache.set(key, model);
    }

    /**
     * List all databases
     */
    async listDatabases() {
        try {
            const entries = await fs.readdir(this.basePath, { withFileTypes: true });
            return entries
                .filter(entry => entry.isDirectory())
                .map(entry => entry.name);
        } catch (err) {
            if (err.code === 'ENOENT') {
                return [];
            }
            throw err;
        }
    }

    /**
     * List all collections in a database
     */
    async listCollections(database) {
        const dbPath = join(this.basePath, database);
        try {
            const entries = await fs.readdir(dbPath);
            return entries
                .filter(entry => entry.endsWith('.json'))
                .map(entry => entry.replace('.json', ''));
        } catch (err) {
            if (err.code === 'ENOENT') {
                return [];
            }
            throw err;
        }
    }

    /**
     * Drop a database (delete all collections)
     */
    async dropDatabase(database) {
        const dbPath = join(this.basePath, database);
        
        // Clear cache for this database
        for (const key of this.modelCache.keys()) {
            if (key.startsWith(`${database}:`)) {
                this.modelCache.delete(key);
            }
        }
        
        // Delete directory
        try {
            await fs.rm(dbPath, { recursive: true, force: true });
            this.logger.info(`Dropped database ${database}`);
        } catch (err) {
            this.logger.error(`Error dropping database ${database}:`, err);
            throw err;
        }
    }

    /**
     * Get statistics for a collection
     */
    async getCollectionStats(database, collection) {
        const model = await this.getModel(database, collection);
        if (!model) {
            return null;
        }

        const modelPath = this.getModelPath(database, collection);
        const stats = await fs.stat(modelPath);
        
        return {
            database,
            collection,
            modelSize: stats.size,
            lastModified: stats.mtime,
            schemaProperties: Object.keys(model.properties || {}).length,
            hasHistograms: !!(model.properties && Object.values(model.properties).some(p => p.histogram)),
            hasStringModels: !!(model.properties && Object.values(model.properties).some(p => p.stringModel)),
            hasTidesConfig: !!model.tidesConfig
        };
    }

    /**
     * Clear all cached models
     */
    clearCache() {
        const size = this.modelCache.size;
        this.modelCache.clear();
        this.logger.info(`Cleared ${size} models from cache`);
    }

    /**
     * Initialize storage (create base directory)
     */
    async initialize() {
        await fs.mkdir(this.basePath, { recursive: true });
        this.logger.info(`Initialized DataFlood storage at ${this.basePath}`);
    }
}

// Export as default
export default DataFloodStorage;