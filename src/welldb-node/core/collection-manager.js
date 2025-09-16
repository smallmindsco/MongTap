/**
 * DataFlood Collection Manager
 * Manages MongoDB collections backed by DataFlood models
 * Integrates with storage for model persistence and generation
 */

import { EventEmitter } from 'events';
import DataFloodStorage from '../storage/dataflood-storage.js';
import { DocumentGenerator } from '../../dataflood-js/generator/document-generator.js';
import { SchemaInferrer } from '../../dataflood-js/schema/inferrer.js';

/**
 * Collection metadata and statistics
 */
class CollectionInfo {
    constructor(database, name) {
        this.database = database;
        this.name = name;
        this.fullName = `${database}.${name}`;
        this.createdAt = new Date();
        this.lastModified = new Date();
        this.documentCount = 0;
        this.indexCount = 0;
        this.indexes = [];
        this.modelTrained = false;
        this.modelVersion = 0;
        this.stats = {
            inserts: 0,
            queries: 0,
            updates: 0,
            deletes: 0,
            generations: 0,
            trainings: 0
        };
    }

    updateStats(operation) {
        if (this.stats[operation] !== undefined) {
            this.stats[operation]++;
        }
        this.lastModified = new Date();
    }
}

/**
 * MongoDB-compatible collection with DataFlood backing
 */
export class Collection extends EventEmitter {
    constructor(database, name, options = {}) {
        super();
        
        this.database = database;
        this.name = name;
        this.fullName = `${database}.${name}`;
        
        // Collection info - Initialize first
        this.info = new CollectionInfo(database, name);
        
        // DataFlood storage
        this.storage = options.storage || new DataFloodStorage();
        
        // Configuration
        this.options = {
            autoTrain: options.autoTrain !== false,
            trainThreshold: options.trainThreshold || 10, // Train after N documents
            maxDocuments: options.maxDocuments || 100000,
            cacheSize: options.cacheSize || 1000,
            generateBatchSize: options.generateBatchSize || 100
        };
        
        // Document cache (for recently generated/inserted docs)
        this.documentCache = [];
        this.pendingTrainingData = [];
        
        // Logging - Initialize before creating indexes
        this.logger = options.logger || this.createDefaultLogger();
        
        // Indexes - Initialize map before creating default index
        this.indexes = new Map();
        
        // Create default _id index after all properties are initialized
        this.createIndex('_id', { unique: true, name: '_id_' });
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
     * Insert documents into collection
     */
    async insert(documents) {
        const docs = Array.isArray(documents) ? documents : [documents];
        
        // Add _id if not present
        for (const doc of docs) {
            if (!doc._id) {
                doc._id = this.generateObjectId();
            }
        }
        
        // Check for duplicate _ids
        if (this.options.enforceUnique) {
            for (const doc of docs) {
                if (this.documentCache.some(d => d._id === doc._id)) {
                    throw new Error(`Duplicate key error: _id ${doc._id} already exists`);
                }
            }
        }
        
        // Add to cache
        this.addToCache(docs);
        
        // Add to pending training data
        if (this.options.autoTrain) {
            this.pendingTrainingData.push(...docs);
            
            // Train if threshold reached
            if (this.pendingTrainingData.length >= this.options.trainThreshold) {
                await this.trainModel();
            }
        }
        
        // Update stats
        this.info.documentCount += docs.length;
        this.info.updateStats('inserts');
        
        this.logger.info(`Inserted ${docs.length} documents into ${this.fullName}`);
        this.emit('insert', { count: docs.length, documents: docs });
        
        return {
            insertedCount: docs.length,
            insertedIds: docs.map(d => d._id)
        };
    }

    /**
     * Find documents in collection
     */
    async find(query = {}, options = {}) {
        const {
            limit = 100,
            skip = 0,
            sort = null,
            projection = null
        } = options;
        
        this.info.updateStats('queries');
        
        // Extract generation control parameters from query
        const generationParams = this.extractGenerationParams(query);
        
        // Remove generation control parameters from query so they don't affect filtering
        const filterQuery = this.removeGenerationParams(query);
        
        // Debug logging
        if (Object.keys(generationParams).length > 0) {
            this.logger.debug('Generation params:', generationParams);
            this.logger.debug('Filter query after removing params:', filterQuery);
        }
        
        // Check if we should generate from model
        const model = await this.storage.getModel(this.database, this.name);
        
        let documents;
        if (model) {
            // Generate from DataFlood model
            const constraints = this.extractConstraints(filterQuery);
            documents = await this.storage.generateDocuments(
                this.database,
                this.name,
                limit + skip,
                { 
                    constraints,
                    seed: generationParams.seed,
                    entropyOverride: generationParams.entropy
                }
            );
            
            this.info.updateStats('generations');
            this.logger.debug(`Generated ${documents.length} documents from model`);
        } else {
            // No model available - return empty array
            documents = [];
            this.logger.debug(`No model found for ${this.database}.${this.name}`);
        }
        
        // Apply query filter (using filterQuery without generation params)
        if (filterQuery && Object.keys(filterQuery).length > 0) {
            documents = this.filterDocuments(documents, filterQuery);
        }
        
        // Apply sort
        if (sort) {
            documents = this.sortDocuments(documents, sort);
        }
        
        // Apply skip and limit
        documents = documents.slice(skip, skip + limit);
        
        // Apply projection
        if (projection) {
            documents = this.projectDocuments(documents, projection);
        }
        
        this.emit('find', { 
            query, 
            count: documents.length,
            options 
        });
        
        return documents;
    }

    /**
     * Find one document
     */
    async findOne(query = {}, options = {}) {
        const results = await this.find(query, { ...options, limit: 1 });
        return results.length > 0 ? results[0] : null;
    }

    /**
     * Update documents in collection
     */
    async update(query, update, options = {}) {
        const {
            multi = false,
            upsert = false
        } = options;
        
        this.info.updateStats('updates');
        
        // Find matching documents
        let documents = await this.find(query, { limit: multi ? 0 : 1 });
        
        if (documents.length === 0 && upsert) {
            // Upsert: create new document
            const newDoc = this.applyUpdate({}, update);
            if (!newDoc._id) {
                newDoc._id = this.generateObjectId();
            }
            await this.insert(newDoc);
            return { matchedCount: 0, modifiedCount: 0, upsertedCount: 1 };
        }
        
        // Apply updates
        let modifiedCount = 0;
        for (const doc of documents) {
            const modified = this.applyUpdate(doc, update);
            if (modified) {
                modifiedCount++;
            }
        }
        
        // Retrain if documents were modified
        if (modifiedCount > 0 && this.options.autoTrain) {
            this.pendingTrainingData.push(...documents);
            if (this.pendingTrainingData.length >= this.options.trainThreshold) {
                await this.trainModel();
            }
        }
        
        this.emit('update', {
            query,
            update,
            matchedCount: documents.length,
            modifiedCount
        });
        
        return {
            matchedCount: documents.length,
            modifiedCount,
            upsertedCount: 0
        };
    }

    /**
     * Delete documents from collection
     */
    async delete(query, options = {}) {
        const { limit = 0 } = options;
        
        this.info.updateStats('deletes');
        
        // Find matching documents
        const documents = await this.find(query, { limit });
        
        // Remove from cache
        for (const doc of documents) {
            const index = this.documentCache.findIndex(d => d._id === doc._id);
            if (index !== -1) {
                this.documentCache.splice(index, 1);
            }
        }
        
        this.info.documentCount = Math.max(0, this.info.documentCount - documents.length);
        
        this.emit('delete', {
            query,
            deletedCount: documents.length
        });
        
        return {
            deletedCount: documents.length
        };
    }

    /**
     * Count documents in collection
     */
    async count(query = {}) {
        // For DataFlood models, return a reasonable count for UI display
        // The actual documents are generated on demand
        const model = await this.storage.getModel(this.database, this.name);
        if (model) {
            // Return a fixed count for collections with models
            return 100;
        }
        
        // No model - return 0
        return 0;
    }

    /**
     * Create an index
     */
    createIndex(fields, options = {}) {
        const indexName = options.name || this.generateIndexName(fields);
        
        const index = {
            name: indexName,
            fields: Array.isArray(fields) ? fields : [fields],
            unique: options.unique || false,
            sparse: options.sparse || false,
            createdAt: new Date()
        };
        
        this.indexes.set(indexName, index);
        this.info.indexes.push(index);
        this.info.indexCount++;
        
        this.logger.info(`Created index ${indexName} on ${this.fullName}`);
        this.emit('indexCreated', index);
        
        return indexName;
    }

    /**
     * Drop an index
     */
    dropIndex(indexName) {
        if (indexName === '_id_') {
            throw new Error('Cannot drop _id index');
        }
        
        if (this.indexes.delete(indexName)) {
            this.info.indexes = this.info.indexes.filter(i => i.name !== indexName);
            this.info.indexCount--;
            
            this.logger.info(`Dropped index ${indexName} on ${this.fullName}`);
            this.emit('indexDropped', indexName);
            return true;
        }
        
        return false;
    }

    /**
     * Get collection statistics
     */
    getStats() {
        return {
            ns: this.fullName,
            count: this.info.documentCount,
            size: this.documentCache.length * 1000, // Rough estimate
            avgObjSize: 1000,
            storageSize: this.documentCache.length * 1200,
            indexes: this.info.indexCount,
            indexSizes: Object.fromEntries(
                Array.from(this.indexes.entries()).map(([name]) => [name, 8192])
            ),
            modelTrained: this.info.modelTrained,
            modelVersion: this.info.modelVersion,
            operations: { ...this.info.stats }
        };
    }

    /**
     * Train DataFlood model with pending data
     */
    async trainModel() {
        if (this.pendingTrainingData.length === 0) {
            return;
        }
        
        try {
            const model = await this.storage.trainModel(
                this.database,
                this.name,
                this.pendingTrainingData
            );
            
            if (model) {
                this.info.modelTrained = true;
                this.info.modelVersion++;
                this.info.updateStats('trainings');
                
                this.logger.info(
                    `Trained model for ${this.fullName} with ${this.pendingTrainingData.length} documents`
                );
                
                this.emit('modelTrained', {
                    documentCount: this.pendingTrainingData.length,
                    version: this.info.modelVersion
                });
            }
            
            // Clear pending data
            this.pendingTrainingData = [];
            
        } catch (err) {
            this.logger.error(`Failed to train model for ${this.fullName}:`, err);
        }
    }

    /**
     * Drop the collection
     */
    async drop() {
        // Delete model
        await this.storage.deleteModel(this.database, this.name);
        
        // Clear data
        this.documentCache = [];
        this.pendingTrainingData = [];
        this.indexes.clear();
        this.createIndex('_id', { unique: true }); // Recreate default index
        
        // Reset info
        this.info = new CollectionInfo(this.database, this.name);
        
        this.logger.info(`Dropped collection ${this.fullName}`);
        this.emit('dropped');
        
        return true;
    }

    /**
     * Extract constraints from query for generation
     */
    extractConstraints(query) {
        const constraints = {};
        
        for (const [field, value] of Object.entries(query)) {
            if (field.startsWith('$')) {
                // Skip logical operators for now
                continue;
            }
            
            if (typeof value === 'object' && value !== null) {
                // Handle MongoDB operators
                const constraint = {};
                
                for (const [op, val] of Object.entries(value)) {
                    switch (op) {
                        case '$eq':
                            constraint.equals = val;
                            break;
                        case '$ne':
                            constraint.notEquals = val;
                            break;
                        case '$gt':
                            constraint.min = val;
                            constraint.excludeMin = true;
                            break;
                        case '$gte':
                            constraint.min = val;
                            break;
                        case '$lt':
                            constraint.max = val;
                            constraint.excludeMax = true;
                            break;
                        case '$lte':
                            constraint.max = val;
                            break;
                        case '$in':
                            constraint.enum = val;
                            break;
                        case '$nin':
                            constraint.notIn = val;
                            break;
                    }
                }
                
                if (Object.keys(constraint).length > 0) {
                    constraints[field] = constraint;
                }
            } else {
                // Direct value match
                constraints[field] = { equals: value };
            }
        }
        
        return constraints;
    }

    /**
     * Extract generation control parameters from query
     */
    extractGenerationParams(query) {
        const params = {};
        
        // Extract $seed parameter
        if (query.$seed !== undefined) {
            params.seed = query.$seed;
        }
        
        // Extract $entropy parameter  
        if (query.$entropy !== undefined) {
            params.entropy = query.$entropy;
        }
        
        // Also support underscore versions for compatibility
        if (query._seed !== undefined) {
            params.seed = query._seed;
        }
        
        if (query._entropy !== undefined) {
            params.entropy = query._entropy;
        }
        
        return params;
    }
    
    /**
     * Remove generation control parameters from query
     */
    removeGenerationParams(query) {
        // Create a copy without generation parameters
        const filtered = {};
        
        for (const [key, value] of Object.entries(query)) {
            // Skip generation control parameters
            if (key === '$seed' || key === '$entropy' || 
                key === '_seed' || key === '_entropy') {
                continue;
            }
            filtered[key] = value;
        }
        
        return filtered;
    }

    /**
     * Filter documents based on query
     */
    filterDocuments(documents, query) {
        return documents.filter(doc => this.matchesQuery(doc, query));
    }

    /**
     * Check if document matches query
     */
    matchesQuery(doc, query) {
        for (const [field, condition] of Object.entries(query)) {
            if (field.startsWith('$')) {
                // Logical operator
                if (!this.matchesLogicalOperator(doc, field, condition)) {
                    return false;
                }
            } else {
                // Field condition
                const value = this.getFieldValue(doc, field);
                if (!this.matchesCondition(value, condition)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Match logical operators
     */
    matchesLogicalOperator(doc, operator, conditions) {
        switch (operator) {
            case '$and':
                return conditions.every(cond => this.matchesQuery(doc, cond));
            case '$or':
                return conditions.some(cond => this.matchesQuery(doc, cond));
            case '$nor':
                return !conditions.some(cond => this.matchesQuery(doc, cond));
            case '$not':
                return !this.matchesQuery(doc, conditions);
            default:
                return true;
        }
    }

    /**
     * Match field condition
     */
    matchesCondition(value, condition) {
        if (this.isOperatorObject(condition)) {
            // Operator-based condition
            for (const [op, operand] of Object.entries(condition)) {
                if (!this.matchesOperator(value, op, operand)) {
                    return false;
                }
            }
            return true;
        } else {
            // Direct equality
            return value === condition;
        }
    }

    /**
     * Match individual operator
     */
    matchesOperator(value, operator, operand) {
        switch (operator) {
            case '$eq': return value === operand;
            case '$ne': return value !== operand;
            case '$gt': return value > operand;
            case '$gte': return value >= operand;
            case '$lt': return value < operand;
            case '$lte': return value <= operand;
            case '$in': return Array.isArray(operand) && operand.includes(value);
            case '$nin': return Array.isArray(operand) && !operand.includes(value);
            case '$exists': return (value !== undefined) === operand;
            case '$type': return typeof value === operand;
            case '$regex': 
                const regex = operand instanceof RegExp ? operand : new RegExp(operand);
                return regex.test(value);
            case '$size':
                return Array.isArray(value) && value.length === operand;
            case '$all':
                return Array.isArray(value) && operand.every(item => value.includes(item));
            default:
                return true;
        }
    }

    /**
     * Sort documents
     */
    sortDocuments(documents, sort) {
        const sortFields = Object.entries(sort);
        
        return documents.sort((a, b) => {
            for (const [field, direction] of sortFields) {
                const aVal = this.getFieldValue(a, field);
                const bVal = this.getFieldValue(b, field);
                
                if (aVal < bVal) return direction === 1 ? -1 : 1;
                if (aVal > bVal) return direction === 1 ? 1 : -1;
            }
            return 0;
        });
    }

    /**
     * Project documents
     */
    projectDocuments(documents, projection) {
        return documents.map(doc => {
            // Handle special MongoDB Compass projections with aggregation expressions
            if (projection.__doc === '$$ROOT' || projection.__size) {
                // Compass wants the full document, ignore the special fields
                // Just handle _id exclusion if specified
                if (projection._id === 0) {
                    const result = { ...doc };
                    delete result._id;
                    return result;
                }
                return doc;
            }
            
            // Standard projection handling
            const projected = {};
            
            // Check if this is an inclusion or exclusion projection
            const fields = Object.keys(projection).filter(k => k !== '_id');
            const hasInclusions = fields.some(f => projection[f] === 1 || projection[f] === true);
            const hasExclusions = fields.some(f => projection[f] === 0 || projection[f] === false);
            
            if (hasExclusions && !hasInclusions) {
                // Exclusion projection: start with full document
                Object.assign(projected, doc);
                
                // Remove excluded fields
                for (const [field, value] of Object.entries(projection)) {
                    if (value === 0 || value === false) {
                        delete projected[field];
                    }
                }
            } else {
                // Inclusion projection: start empty and add specified fields
                // Always include _id unless explicitly excluded
                if (projection._id !== 0) {
                    projected._id = doc._id;
                }
                
                for (const [field, include] of Object.entries(projection)) {
                    if (field !== '_id' && include) {
                        const value = this.getFieldValue(doc, field);
                        if (value !== undefined) {
                            this.setFieldValue(projected, field, value);
                        }
                    }
                }
            }
            
            return projected;
        });
    }

    /**
     * Apply update operations to document
     */
    applyUpdate(doc, update) {
        let modified = false;
        
        for (const [op, fields] of Object.entries(update)) {
            switch (op) {
                case '$set':
                    for (const [field, value] of Object.entries(fields)) {
                        if (this.getFieldValue(doc, field) !== value) {
                            this.setFieldValue(doc, field, value);
                            modified = true;
                        }
                    }
                    break;
                    
                case '$unset':
                    for (const field of Object.keys(fields)) {
                        if (this.deleteFieldValue(doc, field)) {
                            modified = true;
                        }
                    }
                    break;
                    
                case '$inc':
                    for (const [field, amount] of Object.entries(fields)) {
                        const current = this.getFieldValue(doc, field) || 0;
                        this.setFieldValue(doc, field, current + amount);
                        modified = true;
                    }
                    break;
                    
                case '$push':
                    for (const [field, value] of Object.entries(fields)) {
                        const arr = this.getFieldValue(doc, field) || [];
                        if (!Array.isArray(arr)) continue;
                        arr.push(value);
                        this.setFieldValue(doc, field, arr);
                        modified = true;
                    }
                    break;
                    
                case '$pull':
                    for (const [field, value] of Object.entries(fields)) {
                        const arr = this.getFieldValue(doc, field);
                        if (!Array.isArray(arr)) continue;
                        const filtered = arr.filter(item => item !== value);
                        if (filtered.length !== arr.length) {
                            this.setFieldValue(doc, field, filtered);
                            modified = true;
                        }
                    }
                    break;
                    
                default:
                    // Direct replacement if no operators
                    if (!op.startsWith('$')) {
                        Object.assign(doc, update);
                        modified = true;
                    }
            }
        }
        
        return modified;
    }

    /**
     * Get nested field value
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
     * Set nested field value
     */
    setFieldValue(doc, path, value) {
        const parts = path.split('.');
        let current = doc;
        
        for (let i = 0; i < parts.length - 1; i++) {
            const part = parts[i];
            if (!(part in current) || typeof current[part] !== 'object') {
                current[part] = {};
            }
            current = current[part];
        }
        
        current[parts[parts.length - 1]] = value;
    }

    /**
     * Delete nested field
     */
    deleteFieldValue(doc, path) {
        const parts = path.split('.');
        let current = doc;
        
        for (let i = 0; i < parts.length - 1; i++) {
            const part = parts[i];
            if (!(part in current)) return false;
            current = current[part];
        }
        
        const lastPart = parts[parts.length - 1];
        if (lastPart in current) {
            delete current[lastPart];
            return true;
        }
        
        return false;
    }

    /**
     * Check if value is operator object
     */
    isOperatorObject(value) {
        return value && typeof value === 'object' && 
               !Array.isArray(value) &&
               Object.keys(value).some(k => k.startsWith('$'));
    }

    /**
     * Add documents to cache
     */
    addToCache(documents) {
        this.documentCache.push(...documents);
        
        // Trim cache if needed
        if (this.documentCache.length > this.options.cacheSize) {
            this.documentCache = this.documentCache.slice(-this.options.cacheSize);
        }
    }

    /**
     * Generate ObjectId-like string
     */
    generateObjectId() {
        const timestamp = Math.floor(Date.now() / 1000).toString(16);
        const random = Math.random().toString(16).substring(2, 18);
        return (timestamp + random).substring(0, 24).padEnd(24, '0');
    }

    /**
     * Generate index name from fields
     */
    generateIndexName(fields) {
        const fieldList = Array.isArray(fields) ? fields : [fields];
        return fieldList.join('_') + '_1';
    }
}

/**
 * Collection Manager
 */
export class CollectionManager extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.collections = new Map();
        this.storage = options.storage || new DataFloodStorage(options);
        this.logger = options.logger || console;
        
        // Statistics
        this.stats = {
            totalCollections: 0,
            activeCollections: 0,
            totalOperations: 0
        };
    }

    /**
     * Get or create a collection
     */
    async getCollection(database, name, options = {}) {
        const fullName = `${database}.${name}`;
        
        if (this.collections.has(fullName)) {
            return this.collections.get(fullName);
        }
        
        const collection = new Collection(database, name, {
            ...options,
            storage: this.storage,
            logger: this.logger
        });
        
        this.collections.set(fullName, collection);
        this.stats.totalCollections++;
        this.stats.activeCollections++;
        
        // Track operations
        collection.on('insert', () => this.stats.totalOperations++);
        collection.on('find', () => this.stats.totalOperations++);
        collection.on('update', () => this.stats.totalOperations++);
        collection.on('delete', () => this.stats.totalOperations++);
        
        this.logger.info(`Created collection ${fullName}`);
        this.emit('collectionCreated', collection);
        
        return collection;
    }

    /**
     * Drop a collection
     */
    async dropCollection(database, name) {
        const fullName = `${database}.${name}`;
        const collection = this.collections.get(fullName);
        
        if (collection) {
            await collection.drop();
            this.collections.delete(fullName);
            this.stats.activeCollections--;
            
            this.emit('collectionDropped', fullName);
            return true;
        }
        
        return false;
    }

    /**
     * List collections in a database
     */
    async listCollections(database) {
        const collections = [];
        
        for (const [fullName, collection] of this.collections.entries()) {
            if (collection.database === database) {
                collections.push({
                    name: collection.name,
                    type: 'collection',
                    info: collection.info
                });
            }
        }
        
        // Also check storage for persisted models
        const storedCollections = await this.storage.listCollections(database);
        for (const name of storedCollections) {
            const fullName = `${database}.${name}`;
            if (!this.collections.has(fullName)) {
                collections.push({
                    name,
                    type: 'collection',
                    persisted: true
                });
            }
        }
        
        return collections;
    }

    /**
     * Get statistics
     */
    getStats() {
        const collectionStats = [];
        
        for (const collection of this.collections.values()) {
            collectionStats.push(collection.getStats());
        }
        
        return {
            ...this.stats,
            collections: collectionStats
        };
    }

    /**
     * Clear all collections
     */
    clear() {
        for (const collection of this.collections.values()) {
            collection.drop();
        }
        this.collections.clear();
        this.stats.activeCollections = 0;
    }
}

// Export everything
export default CollectionManager;