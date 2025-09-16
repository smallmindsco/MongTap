/**
 * Query Engine with DataFlood Integration
 * 
 * This query engine handles MongoDB queries by:
 * 1. Parsing query constraints
 * 2. Generating documents from DataFlood models
 * 3. Filtering generated documents based on constraints
 * 4. Supporting MongoDB query operators
 */

import { EventEmitter } from 'events';

/**
 * MongoDB Query Engine with DataFlood document generation
 */
export class QueryEngine extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.options = {
            maxGeneratedDocs: options.maxGeneratedDocs || 10000,
            batchSize: options.batchSize || 100,
            timeout: options.timeout || 30000,
            constraintOptimization: options.constraintOptimization !== false,
            cacheResults: options.cacheResults !== false
        };
        
        // Result cache
        this.cache = new Map();
        this.cacheHits = 0;
        this.cacheMisses = 0;
        
        // Statistics
        this.stats = {
            queries: 0,
            totalGenerated: 0,
            totalFiltered: 0,
            averageGenerationTime: 0,
            averageFilterTime: 0
        };
        
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
     * Execute a query against a collection
     */
    async executeQuery(collection, query = {}, options = {}) {
        const startTime = Date.now();
        this.stats.queries++;
        
        // Check cache
        const cacheKey = this.getCacheKey(collection.fullName, query, options);
        if (this.options.cacheResults && this.cache.has(cacheKey)) {
            this.cacheHits++;
            this.logger.debug(`Cache hit for query ${cacheKey}`);
            return this.cache.get(cacheKey);
        }
        this.cacheMisses++;
        
        try {
            // Extract constraints from query
            const constraints = this.extractConstraints(query);
            
            // Determine how many documents to generate
            const limit = options.limit || 100;
            const skip = options.skip || 0;
            
            // Generate documents based on constraints
            const generatedDocs = await this.generateDocuments(
                collection,
                limit + skip,
                constraints
            );
            
            // Filter documents based on query
            const filteredDocs = this.filterDocuments(generatedDocs, query);
            
            // Apply skip and limit
            let results = filteredDocs.slice(skip, skip + limit);
            
            // Apply sort if specified
            if (options.sort) {
                results = this.sortDocuments(results, options.sort);
            }
            
            // Apply projection if specified
            if (options.projection) {
                results = this.projectDocuments(results, options.projection);
            }
            
            // Update statistics
            const endTime = Date.now();
            this.updateStats(generatedDocs.length, filteredDocs.length, endTime - startTime);
            
            // Cache results
            if (this.options.cacheResults) {
                this.cache.set(cacheKey, results);
                
                // Limit cache size
                if (this.cache.size > 1000) {
                    const firstKey = this.cache.keys().next().value;
                    this.cache.delete(firstKey);
                }
            }
            
            this.logger.debug(
                `Query executed: generated=${generatedDocs.length}, ` +
                `filtered=${filteredDocs.length}, returned=${results.length}`
            );
            
            return results;
            
        } catch (error) {
            this.logger.error('Query execution error:', error);
            throw error;
        }
    }
    
    /**
     * Extract constraints from MongoDB query for optimized generation
     */
    extractConstraints(query) {
        const constraints = {};
        
        for (const [field, value] of Object.entries(query)) {
            if (field.startsWith('$')) {
                // Logical operator
                continue;
            }
            
            if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                // Parse operators
                for (const [op, val] of Object.entries(value)) {
                    switch (op) {
                        case '$eq':
                            constraints[field] = { equals: val };
                            break;
                        case '$ne':
                            constraints[field] = { notEquals: val };
                            break;
                        case '$gt':
                            if (!constraints[field]) constraints[field] = {};
                            constraints[field].min = val;
                            constraints[field].excludeMin = true;
                            break;
                        case '$gte':
                            if (!constraints[field]) constraints[field] = {};
                            constraints[field].min = val;
                            break;
                        case '$lt':
                            if (!constraints[field]) constraints[field] = {};
                            constraints[field].max = val;
                            constraints[field].excludeMax = true;
                            break;
                        case '$lte':
                            if (!constraints[field]) constraints[field] = {};
                            constraints[field].max = val;
                            break;
                        case '$in':
                            constraints[field] = { values: val };
                            break;
                        case '$nin':
                            constraints[field] = { excludeValues: val };
                            break;
                        case '$regex':
                            constraints[field] = { pattern: val };
                            break;
                        case '$exists':
                            constraints[field] = { exists: val };
                            break;
                    }
                }
            } else {
                // Direct equality
                constraints[field] = { equals: value };
            }
        }
        
        return constraints;
    }
    
    /**
     * Generate documents from DataFlood model with constraints
     */
    async generateDocuments(collection, count, constraints) {
        const genStart = Date.now();
        
        // Use collection's storage to generate documents
        const documents = await collection.storage.generateDocuments(
            collection.database,
            collection.name,
            count,
            { constraints }
        );
        
        const genTime = Date.now() - genStart;
        this.logger.debug(`Generated ${documents.length} documents in ${genTime}ms`);
        
        return documents;
    }
    
    /**
     * Filter documents based on MongoDB query
     */
    filterDocuments(documents, query) {
        if (!query || Object.keys(query).length === 0) {
            return documents;
        }
        
        const filterStart = Date.now();
        const filtered = documents.filter(doc => this.matchesQuery(doc, query));
        
        const filterTime = Date.now() - filterStart;
        this.logger.debug(`Filtered to ${filtered.length} documents in ${filterTime}ms`);
        
        return filtered;
    }
    
    /**
     * Check if a document matches a query
     */
    matchesQuery(doc, query) {
        for (const [field, condition] of Object.entries(query)) {
            // Handle logical operators
            if (field === '$and') {
                if (!condition.every(subQuery => this.matchesQuery(doc, subQuery))) {
                    return false;
                }
                continue;
            }
            
            if (field === '$or') {
                if (!condition.some(subQuery => this.matchesQuery(doc, subQuery))) {
                    return false;
                }
                continue;
            }
            
            if (field === '$nor') {
                if (condition.some(subQuery => this.matchesQuery(doc, subQuery))) {
                    return false;
                }
                continue;
            }
            
            if (field === '$not') {
                if (this.matchesQuery(doc, condition)) {
                    return false;
                }
                continue;
            }
            
            // Handle field conditions
            const fieldValue = this.getFieldValue(doc, field);
            
            if (!this.matchesCondition(fieldValue, condition)) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Get nested field value from document
     */
    getFieldValue(doc, path) {
        const parts = path.split('.');
        let value = doc;
        
        for (const part of parts) {
            if (value == null) {
                return undefined;
            }
            value = value[part];
        }
        
        return value;
    }
    
    /**
     * Check if a value matches a condition
     */
    matchesCondition(value, condition) {
        // Direct equality
        if (condition === null || condition === undefined) {
            return value == condition;
        }
        
        if (typeof condition !== 'object' || Array.isArray(condition)) {
            return this.equals(value, condition);
        }
        
        // Handle operators
        for (const [op, expected] of Object.entries(condition)) {
            switch (op) {
                case '$eq':
                    if (!this.equals(value, expected)) return false;
                    break;
                case '$ne':
                    if (this.equals(value, expected)) return false;
                    break;
                case '$gt':
                    if (value <= expected) return false;
                    break;
                case '$gte':
                    if (value < expected) return false;
                    break;
                case '$lt':
                    if (value >= expected) return false;
                    break;
                case '$lte':
                    if (value > expected) return false;
                    break;
                case '$in':
                    if (!expected.some(v => this.equals(value, v))) return false;
                    break;
                case '$nin':
                    if (expected.some(v => this.equals(value, v))) return false;
                    break;
                case '$exists':
                    if ((value !== undefined) !== expected) return false;
                    break;
                case '$type':
                    if (!this.matchesType(value, expected)) return false;
                    break;
                case '$regex':
                    if (!this.matchesRegex(value, expected, condition.$options)) return false;
                    break;
                case '$size':
                    if (!Array.isArray(value) || value.length !== expected) return false;
                    break;
                case '$all':
                    if (!Array.isArray(value) || !expected.every(e => value.includes(e))) return false;
                    break;
                case '$elemMatch':
                    if (!Array.isArray(value) || !value.some(v => this.matchesQuery({item: v}, {item: expected}))) return false;
                    break;
            }
        }
        
        return true;
    }
    
    /**
     * Compare values for equality
     */
    equals(a, b) {
        if (a === b) return true;
        if (a == null || b == null) return false;
        
        // Handle dates
        if (a instanceof Date && b instanceof Date) {
            return a.getTime() === b.getTime();
        }
        
        // Handle arrays
        if (Array.isArray(a) && Array.isArray(b)) {
            if (a.length !== b.length) return false;
            return a.every((v, i) => this.equals(v, b[i]));
        }
        
        // Handle objects
        if (typeof a === 'object' && typeof b === 'object') {
            const keysA = Object.keys(a);
            const keysB = Object.keys(b);
            if (keysA.length !== keysB.length) return false;
            return keysA.every(key => this.equals(a[key], b[key]));
        }
        
        return false;
    }
    
    /**
     * Check if value matches type
     */
    matchesType(value, type) {
        const typeMap = {
            'double': 'number',
            'string': 'string',
            'object': 'object',
            'array': Array.isArray,
            'bool': 'boolean',
            'null': v => v === null,
            'int': v => Number.isInteger(v),
            'date': v => v instanceof Date
        };
        
        if (typeof type === 'string') {
            const checker = typeMap[type.toLowerCase()];
            if (typeof checker === 'function') {
                return checker(value);
            }
            return typeof value === checker;
        }
        
        return false;
    }
    
    /**
     * Check if value matches regex
     */
    matchesRegex(value, pattern, options = '') {
        if (typeof value !== 'string') return false;
        
        try {
            const regex = new RegExp(pattern, options);
            return regex.test(value);
        } catch (e) {
            return false;
        }
    }
    
    /**
     * Sort documents
     */
    sortDocuments(documents, sortSpec) {
        return documents.sort((a, b) => {
            for (const [field, direction] of Object.entries(sortSpec)) {
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
        return documents.map(doc => this.projectDocument(doc, projection));
    }
    
    /**
     * Project a single document
     */
    projectDocument(doc, projection) {
        const result = {};
        const includeMode = Object.values(projection).some(v => v === 1);
        
        if (includeMode) {
            // Include only specified fields
            for (const [field, include] of Object.entries(projection)) {
                if (include === 1) {
                    const value = this.getFieldValue(doc, field);
                    if (value !== undefined) {
                        this.setFieldValue(result, field, value);
                    }
                }
            }
            
            // Always include _id unless explicitly excluded
            if (projection._id !== 0 && doc._id !== undefined) {
                result._id = doc._id;
            }
        } else {
            // Exclude specified fields
            Object.assign(result, doc);
            
            for (const [field, exclude] of Object.entries(projection)) {
                if (exclude === 0) {
                    this.deleteFieldValue(result, field);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Set nested field value
     */
    setFieldValue(obj, path, value) {
        const parts = path.split('.');
        const last = parts.pop();
        
        let current = obj;
        for (const part of parts) {
            if (!current[part]) {
                current[part] = {};
            }
            current = current[part];
        }
        
        current[last] = value;
    }
    
    /**
     * Delete nested field value
     */
    deleteFieldValue(obj, path) {
        const parts = path.split('.');
        const last = parts.pop();
        
        let current = obj;
        for (const part of parts) {
            if (!current[part]) return;
            current = current[part];
        }
        
        delete current[last];
    }
    
    /**
     * Generate cache key
     */
    getCacheKey(collection, query, options) {
        return JSON.stringify({ collection, query, options });
    }
    
    /**
     * Update statistics
     */
    updateStats(generated, filtered, time) {
        this.stats.totalGenerated += generated;
        this.stats.totalFiltered += filtered;
        
        const prevAvgGen = this.stats.averageGenerationTime;
        const prevAvgFilter = this.stats.averageFilterTime;
        const n = this.stats.queries;
        
        this.stats.averageGenerationTime = (prevAvgGen * (n - 1) + time) / n;
        this.stats.averageFilterTime = (prevAvgFilter * (n - 1) + time) / n;
    }
    
    /**
     * Clear cache
     */
    clearCache() {
        this.cache.clear();
        this.cacheHits = 0;
        this.cacheMisses = 0;
    }
    
    /**
     * Get statistics
     */
    getStats() {
        return {
            ...this.stats,
            cacheHits: this.cacheHits,
            cacheMisses: this.cacheMisses,
            cacheHitRate: this.cacheHits / (this.cacheHits + this.cacheMisses) || 0,
            cacheSize: this.cache.size
        };
    }
    
    /**
     * Execute aggregation pipeline
     */
    async executeAggregation(collection, pipeline, options = {}) {
        let documents = await this.executeQuery(collection, {}, { limit: 10000 });
        
        for (const stage of pipeline) {
            const [stageName, stageConfig] = Object.entries(stage)[0];
            
            switch (stageName) {
                case '$match':
                    documents = this.filterDocuments(documents, stageConfig);
                    break;
                    
                case '$project':
                    documents = this.projectDocuments(documents, stageConfig);
                    break;
                    
                case '$sort':
                    documents = this.sortDocuments(documents, stageConfig);
                    break;
                    
                case '$limit':
                    documents = documents.slice(0, stageConfig);
                    break;
                    
                case '$skip':
                    documents = documents.slice(stageConfig);
                    break;
                    
                case '$group':
                    documents = this.groupDocuments(documents, stageConfig);
                    break;
                    
                case '$unwind':
                    documents = this.unwindDocuments(documents, stageConfig);
                    break;
                    
                default:
                    this.logger.warn(`Unsupported aggregation stage: ${stageName}`);
            }
        }
        
        return documents;
    }
    
    /**
     * Group documents for aggregation
     */
    groupDocuments(documents, groupSpec) {
        const groups = new Map();
        
        for (const doc of documents) {
            // Calculate group key
            const key = this.calculateGroupKey(doc, groupSpec._id);
            
            if (!groups.has(key)) {
                groups.set(key, []);
            }
            groups.get(key).push(doc);
        }
        
        // Build result documents
        const results = [];
        for (const [key, groupDocs] of groups) {
            const result = { _id: key };
            
            // Calculate aggregations
            for (const [field, spec] of Object.entries(groupSpec)) {
                if (field === '_id') continue;
                
                const [op, expression] = Object.entries(spec)[0];
                
                switch (op) {
                    case '$sum':
                        result[field] = this.calculateSum(groupDocs, expression);
                        break;
                    case '$avg':
                        result[field] = this.calculateAvg(groupDocs, expression);
                        break;
                    case '$min':
                        result[field] = this.calculateMin(groupDocs, expression);
                        break;
                    case '$max':
                        result[field] = this.calculateMax(groupDocs, expression);
                        break;
                    case '$count':
                        result[field] = groupDocs.length;
                        break;
                    case '$first':
                        result[field] = this.getFieldValue(groupDocs[0], expression.substring(1));
                        break;
                    case '$last':
                        result[field] = this.getFieldValue(groupDocs[groupDocs.length - 1], expression.substring(1));
                        break;
                }
            }
            
            results.push(result);
        }
        
        return results;
    }
    
    /**
     * Calculate group key
     */
    calculateGroupKey(doc, keySpec) {
        if (keySpec === null) return null;
        if (typeof keySpec === 'string' && keySpec.startsWith('$')) {
            return this.getFieldValue(doc, keySpec.substring(1));
        }
        if (typeof keySpec === 'object') {
            const key = {};
            for (const [field, expr] of Object.entries(keySpec)) {
                if (typeof expr === 'string' && expr.startsWith('$')) {
                    key[field] = this.getFieldValue(doc, expr.substring(1));
                } else {
                    key[field] = expr;
                }
            }
            return JSON.stringify(key);
        }
        return keySpec;
    }
    
    /**
     * Calculate sum for aggregation
     */
    calculateSum(docs, expression) {
        if (expression === 1) return docs.length;
        
        const field = expression.substring(1);
        return docs.reduce((sum, doc) => {
            const val = this.getFieldValue(doc, field);
            return sum + (typeof val === 'number' ? val : 0);
        }, 0);
    }
    
    /**
     * Calculate average for aggregation
     */
    calculateAvg(docs, expression) {
        const sum = this.calculateSum(docs, expression);
        return docs.length > 0 ? sum / docs.length : 0;
    }
    
    /**
     * Calculate min for aggregation
     */
    calculateMin(docs, expression) {
        const field = expression.substring(1);
        let min = Infinity;
        
        for (const doc of docs) {
            const val = this.getFieldValue(doc, field);
            if (typeof val === 'number' && val < min) {
                min = val;
            }
        }
        
        return min === Infinity ? null : min;
    }
    
    /**
     * Calculate max for aggregation
     */
    calculateMax(docs, expression) {
        const field = expression.substring(1);
        let max = -Infinity;
        
        for (const doc of docs) {
            const val = this.getFieldValue(doc, field);
            if (typeof val === 'number' && val > max) {
                max = val;
            }
        }
        
        return max === -Infinity ? null : max;
    }
    
    /**
     * Unwind documents for aggregation
     */
    unwindDocuments(documents, unwindSpec) {
        const results = [];
        const field = typeof unwindSpec === 'string' ? unwindSpec : unwindSpec.path;
        const fieldName = field.substring(1); // Remove $
        
        for (const doc of documents) {
            const array = this.getFieldValue(doc, fieldName);
            
            if (!Array.isArray(array) || array.length === 0) {
                if (unwindSpec.preserveNullAndEmptyArrays) {
                    results.push(doc);
                }
                continue;
            }
            
            for (const item of array) {
                const newDoc = { ...doc };
                this.setFieldValue(newDoc, fieldName, item);
                results.push(newDoc);
            }
        }
        
        return results;
    }
}

export default QueryEngine;