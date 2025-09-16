/**
 * MongoDB Aggregation Pipeline Implementation
 * 
 * Provides a complete aggregation framework for processing documents:
 * - Pipeline stages ($match, $group, $sort, $project, etc.)
 * - Aggregation expressions and operators
 * - Statistical and mathematical functions
 * - Array and string manipulation
 */

import { EventEmitter } from 'events';

/**
 * Aggregation Pipeline Executor
 */
export class AggregationPipeline extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.options = {
            maxDocuments: options.maxDocuments || 100000,
            maxMemory: options.maxMemory || 100 * 1024 * 1024, // 100MB
            allowDiskUse: options.allowDiskUse || false,
            explain: options.explain || false
        };
        
        // Statistics
        this.stats = {
            pipelines: 0,
            stages: 0,
            documentsProcessed: 0,
            executionTime: 0,
            memoryUsed: 0
        };
        
        // Stage handlers
        this.stageHandlers = new Map([
            ['$match', this.stageMatch.bind(this)],
            ['$project', this.stageProject.bind(this)],
            ['$group', this.stageGroup.bind(this)],
            ['$sort', this.stageSort.bind(this)],
            ['$limit', this.stageLimit.bind(this)],
            ['$skip', this.stageSkip.bind(this)],
            ['$unwind', this.stageUnwind.bind(this)],
            ['$lookup', this.stageLookup.bind(this)],
            ['$addFields', this.stageAddFields.bind(this)],
            ['$set', this.stageAddFields.bind(this)], // Alias for $addFields
            ['$unset', this.stageUnset.bind(this)],
            ['$replaceRoot', this.stageReplaceRoot.bind(this)],
            ['$replaceWith', this.stageReplaceRoot.bind(this)], // Alias
            ['$count', this.stageCount.bind(this)],
            ['$facet', this.stageFacet.bind(this)],
            ['$bucket', this.stageBucket.bind(this)],
            ['$bucketAuto', this.stageBucketAuto.bind(this)],
            ['$sample', this.stageSample.bind(this)],
            ['$merge', this.stageMerge.bind(this)],
            ['$out', this.stageOut.bind(this)]
        ]);
        
        // Expression evaluator
        this.expressionEvaluator = new ExpressionEvaluator();
        
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
     * Execute aggregation pipeline
     */
    async execute(documents, pipeline, context = {}) {
        const startTime = Date.now();
        this.stats.pipelines++;
        
        if (!Array.isArray(pipeline)) {
            throw new Error('Pipeline must be an array');
        }
        
        let current = Array.from(documents);
        const executionPlan = [];
        
        try {
            // Process each stage
            for (const stage of pipeline) {
                const stageName = Object.keys(stage)[0];
                const stageConfig = stage[stageName];
                
                if (!this.stageHandlers.has(stageName)) {
                    throw new Error(`Unknown pipeline stage: ${stageName}`);
                }
                
                this.logger.debug(`Executing stage ${stageName} on ${current.length} documents`);
                
                const stageStart = Date.now();
                const handler = this.stageHandlers.get(stageName);
                current = await handler(current, stageConfig, context);
                const stageTime = Date.now() - stageStart;
                
                executionPlan.push({
                    stage: stageName,
                    inputDocs: documents.length,
                    outputDocs: current.length,
                    executionTime: stageTime
                });
                
                this.stats.stages++;
                this.stats.documentsProcessed += current.length;
                
                // Check limits
                if (current.length > this.options.maxDocuments) {
                    throw new Error(`Document limit exceeded: ${current.length} > ${this.options.maxDocuments}`);
                }
            }
            
            const totalTime = Date.now() - startTime;
            this.stats.executionTime += totalTime;
            
            if (this.options.explain) {
                return {
                    result: current,
                    executionStats: {
                        executionTimeMillis: totalTime,
                        totalDocsExamined: documents.length,
                        totalDocsReturned: current.length,
                        executionStages: executionPlan
                    }
                };
            }
            
            return current;
            
        } catch (error) {
            this.logger.error('Pipeline execution error:', error);
            throw error;
        }
    }
    
    /**
     * $match stage - Filter documents
     */
    async stageMatch(documents, filter) {
        return documents.filter(doc => this.matchesFilter(doc, filter));
    }
    
    /**
     * $project stage - Reshape documents
     */
    async stageProject(documents, projection) {
        return documents.map(doc => this.projectDocument(doc, projection));
    }
    
    /**
     * $group stage - Group documents
     */
    async stageGroup(documents, groupSpec) {
        const groups = new Map();
        
        // Group documents
        for (const doc of documents) {
            const key = this.expressionEvaluator.evaluate(groupSpec._id, doc);
            const keyStr = JSON.stringify(key);
            
            if (!groups.has(keyStr)) {
                groups.set(keyStr, []);
            }
            groups.get(keyStr).push(doc);
        }
        
        // Build result documents
        const results = [];
        for (const [keyStr, groupDocs] of groups) {
            const key = JSON.parse(keyStr);
            const result = { _id: key };
            
            // Calculate accumulator fields
            for (const [field, accumulator] of Object.entries(groupSpec)) {
                if (field === '_id') continue;
                
                result[field] = this.calculateAccumulator(accumulator, groupDocs);
            }
            
            results.push(result);
        }
        
        return results;
    }
    
    /**
     * $sort stage - Sort documents
     */
    async stageSort(documents, sortSpec) {
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
     * $limit stage - Limit number of documents
     */
    async stageLimit(documents, limit) {
        return documents.slice(0, limit);
    }
    
    /**
     * $skip stage - Skip documents
     */
    async stageSkip(documents, skip) {
        return documents.slice(skip);
    }
    
    /**
     * $unwind stage - Deconstruct array field
     */
    async stageUnwind(documents, unwindSpec) {
        const results = [];
        const path = typeof unwindSpec === 'string' ? unwindSpec : unwindSpec.path;
        const fieldName = path.startsWith('$') ? path.substring(1) : path;
        const preserveNull = unwindSpec.preserveNullAndEmptyArrays || false;
        const includeIndex = unwindSpec.includeArrayIndex;
        
        for (const doc of documents) {
            const array = this.getFieldValue(doc, fieldName);
            
            if (!Array.isArray(array) || array.length === 0) {
                if (preserveNull) {
                    results.push(doc);
                }
                continue;
            }
            
            array.forEach((item, index) => {
                const newDoc = { ...doc };
                this.setFieldValue(newDoc, fieldName, item);
                
                if (includeIndex) {
                    newDoc[includeIndex] = index;
                }
                
                results.push(newDoc);
            });
        }
        
        return results;
    }
    
    /**
     * $lookup stage - Join with another collection
     */
    async stageLookup(documents, lookupSpec, context) {
        const { from, localField, foreignField, as } = lookupSpec;
        
        // Get foreign collection (would need collection manager in real implementation)
        const foreignDocs = context.collections ? context.collections[from] || [] : [];
        
        return documents.map(doc => {
            const localValue = this.getFieldValue(doc, localField);
            const matched = foreignDocs.filter(foreign => 
                this.getFieldValue(foreign, foreignField) === localValue
            );
            
            return {
                ...doc,
                [as]: matched
            };
        });
    }
    
    /**
     * $addFields stage - Add new fields
     */
    async stageAddFields(documents, fields) {
        return documents.map(doc => {
            const newDoc = { ...doc };
            
            for (const [field, expression] of Object.entries(fields)) {
                newDoc[field] = this.expressionEvaluator.evaluate(expression, doc);
            }
            
            return newDoc;
        });
    }
    
    /**
     * $unset stage - Remove fields
     */
    async stageUnset(documents, fields) {
        const fieldList = Array.isArray(fields) ? fields : [fields];
        
        return documents.map(doc => {
            const newDoc = { ...doc };
            
            for (const field of fieldList) {
                this.deleteFieldValue(newDoc, field);
            }
            
            return newDoc;
        });
    }
    
    /**
     * $replaceRoot stage - Replace document root
     */
    async stageReplaceRoot(documents, replaceSpec) {
        const newRoot = replaceSpec.newRoot || replaceSpec;
        
        return documents.map(doc => {
            if (typeof newRoot === 'string' && newRoot.startsWith('$')) {
                // Use field value as new root
                return this.getFieldValue(doc, newRoot.substring(1)) || {};
            }
            
            // Evaluate expression as new root
            return this.expressionEvaluator.evaluate(newRoot, doc);
        });
    }
    
    /**
     * $count stage - Count documents
     */
    async stageCount(documents, fieldName) {
        return [{ [fieldName]: documents.length }];
    }
    
    /**
     * $facet stage - Multiple pipelines
     */
    async stageFacet(documents, facetSpec) {
        const result = {};
        
        for (const [name, pipeline] of Object.entries(facetSpec)) {
            result[name] = await this.execute(documents, pipeline);
        }
        
        return [result];
    }
    
    /**
     * $bucket stage - Categorize into buckets
     */
    async stageBucket(documents, bucketSpec) {
        const { groupBy, boundaries, default: defaultBucket, output } = bucketSpec;
        const buckets = new Map();
        
        // Initialize buckets
        for (let i = 0; i < boundaries.length - 1; i++) {
            buckets.set(boundaries[i], []);
        }
        if (defaultBucket !== undefined) {
            buckets.set(defaultBucket, []);
        }
        
        // Categorize documents
        for (const doc of documents) {
            const value = this.expressionEvaluator.evaluate(groupBy, doc);
            let placed = false;
            
            for (let i = 0; i < boundaries.length - 1; i++) {
                if (value >= boundaries[i] && value < boundaries[i + 1]) {
                    buckets.get(boundaries[i]).push(doc);
                    placed = true;
                    break;
                }
            }
            
            if (!placed && defaultBucket !== undefined) {
                buckets.get(defaultBucket).push(doc);
            }
        }
        
        // Build results
        const results = [];
        for (const [boundary, docs] of buckets) {
            if (docs.length > 0) {
                const bucket = { _id: boundary, count: docs.length };
                
                if (output) {
                    for (const [field, accumulator] of Object.entries(output)) {
                        bucket[field] = this.calculateAccumulator(accumulator, docs);
                    }
                }
                
                results.push(bucket);
            }
        }
        
        return results;
    }
    
    /**
     * $bucketAuto stage - Auto-generate buckets
     */
    async stageBucketAuto(documents, bucketSpec) {
        const { groupBy, buckets: numBuckets, output } = bucketSpec;
        
        // Calculate values and sort
        const values = documents.map(doc => ({
            value: this.expressionEvaluator.evaluate(groupBy, doc),
            doc
        })).sort((a, b) => a.value - b.value);
        
        if (values.length === 0) return [];
        
        // Create buckets
        const bucketSize = Math.ceil(values.length / numBuckets);
        const results = [];
        
        for (let i = 0; i < numBuckets && i * bucketSize < values.length; i++) {
            const start = i * bucketSize;
            const end = Math.min((i + 1) * bucketSize, values.length);
            const bucketDocs = values.slice(start, end).map(v => v.doc);
            
            if (bucketDocs.length > 0) {
                const bucket = {
                    _id: {
                        min: values[start].value,
                        max: values[end - 1].value
                    },
                    count: bucketDocs.length
                };
                
                if (output) {
                    for (const [field, accumulator] of Object.entries(output)) {
                        bucket[field] = this.calculateAccumulator(accumulator, bucketDocs);
                    }
                }
                
                results.push(bucket);
            }
        }
        
        return results;
    }
    
    /**
     * $sample stage - Random sample
     */
    async stageSample(documents, sampleSpec) {
        const size = sampleSpec.size;
        
        if (size >= documents.length) {
            return documents;
        }
        
        // Fisher-Yates shuffle for random sampling
        const result = [...documents];
        for (let i = result.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [result[i], result[j]] = [result[j], result[i]];
        }
        
        return result.slice(0, size);
    }
    
    /**
     * $merge stage - Merge into collection
     */
    async stageMerge(documents, mergeSpec, context) {
        // In real implementation, would merge into target collection
        this.logger.info(`Would merge ${documents.length} documents into ${mergeSpec.into}`);
        return documents;
    }
    
    /**
     * $out stage - Output to collection
     */
    async stageOut(documents, collection, context) {
        // In real implementation, would output to target collection
        this.logger.info(`Would output ${documents.length} documents to ${collection}`);
        return documents;
    }
    
    /**
     * Calculate accumulator value
     */
    calculateAccumulator(accumulator, documents) {
        const [op, expression] = Object.entries(accumulator)[0];
        
        switch (op) {
            case '$sum':
                return this.accumulatorSum(documents, expression);
            case '$avg':
                return this.accumulatorAvg(documents, expression);
            case '$min':
                return this.accumulatorMin(documents, expression);
            case '$max':
                return this.accumulatorMax(documents, expression);
            case '$count':
                return documents.length;
            case '$first':
                return documents.length > 0 
                    ? this.expressionEvaluator.evaluate(expression, documents[0])
                    : null;
            case '$last':
                return documents.length > 0 
                    ? this.expressionEvaluator.evaluate(expression, documents[documents.length - 1])
                    : null;
            case '$push':
                return documents.map(doc => 
                    this.expressionEvaluator.evaluate(expression, doc)
                );
            case '$addToSet':
                const set = new Set();
                for (const doc of documents) {
                    set.add(JSON.stringify(
                        this.expressionEvaluator.evaluate(expression, doc)
                    ));
                }
                return Array.from(set).map(s => JSON.parse(s));
            case '$stdDevPop':
                return this.accumulatorStdDev(documents, expression, false);
            case '$stdDevSamp':
                return this.accumulatorStdDev(documents, expression, true);
            default:
                throw new Error(`Unknown accumulator: ${op}`);
        }
    }
    
    accumulatorSum(documents, expression) {
        if (expression === 1) return documents.length;
        
        return documents.reduce((sum, doc) => {
            const val = this.expressionEvaluator.evaluate(expression, doc);
            return sum + (typeof val === 'number' ? val : 0);
        }, 0);
    }
    
    accumulatorAvg(documents, expression) {
        if (documents.length === 0) return null;
        return this.accumulatorSum(documents, expression) / documents.length;
    }
    
    accumulatorMin(documents, expression) {
        if (documents.length === 0) return null;
        
        return documents.reduce((min, doc) => {
            const val = this.expressionEvaluator.evaluate(expression, doc);
            return val < min ? val : min;
        }, Infinity);
    }
    
    accumulatorMax(documents, expression) {
        if (documents.length === 0) return null;
        
        return documents.reduce((max, doc) => {
            const val = this.expressionEvaluator.evaluate(expression, doc);
            return val > max ? val : max;
        }, -Infinity);
    }
    
    accumulatorStdDev(documents, expression, sample) {
        if (documents.length === 0) return null;
        if (sample && documents.length === 1) return null;
        
        const values = documents.map(doc => 
            this.expressionEvaluator.evaluate(expression, doc)
        ).filter(v => typeof v === 'number');
        
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const variance = values.reduce((sum, val) => 
            sum + Math.pow(val - mean, 2), 0
        ) / (sample ? values.length - 1 : values.length);
        
        return Math.sqrt(variance);
    }
    
    /**
     * Helper methods
     */
    matchesFilter(doc, filter) {
        // Simple filter matching (would use full query engine in real implementation)
        for (const [field, condition] of Object.entries(filter)) {
            const value = this.getFieldValue(doc, field);
            
            if (typeof condition === 'object' && condition !== null) {
                // Handle operators
                for (const [op, expected] of Object.entries(condition)) {
                    switch (op) {
                        case '$eq':
                            if (value !== expected) return false;
                            break;
                        case '$ne':
                            if (value === expected) return false;
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
                            if (!expected.includes(value)) return false;
                            break;
                        case '$nin':
                            if (expected.includes(value)) return false;
                            break;
                    }
                }
            } else {
                // Direct equality
                if (value !== condition) return false;
            }
        }
        
        return true;
    }
    
    projectDocument(doc, projection) {
        const result = {};
        
        for (const [field, spec] of Object.entries(projection)) {
            if (spec === 1) {
                // Include field
                result[field] = this.getFieldValue(doc, field);
            } else if (spec === 0) {
                // Exclude field (handled separately)
            } else {
                // Expression
                result[field] = this.expressionEvaluator.evaluate(spec, doc);
            }
        }
        
        return result;
    }
    
    getFieldValue(doc, path) {
        if (path === '$$ROOT') return doc;
        if (path === '$$CURRENT') return doc;
        
        const parts = path.split('.');
        let value = doc;
        
        for (const part of parts) {
            if (value == null) return undefined;
            value = value[part];
        }
        
        return value;
    }
    
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
     * Get statistics
     */
    getStats() {
        return { ...this.stats };
    }
}

/**
 * Expression Evaluator for aggregation expressions
 */
class ExpressionEvaluator {
    evaluate(expression, document) {
        if (expression === null || expression === undefined) {
            return expression;
        }
        
        // String starting with $ is field reference
        if (typeof expression === 'string' && expression.startsWith('$')) {
            if (expression === '$$ROOT') return document;
            if (expression === '$$CURRENT') return document;
            
            const fieldPath = expression.substring(1);
            return this.getFieldValue(document, fieldPath);
        }
        
        // Object might be an operator expression
        if (typeof expression === 'object' && !Array.isArray(expression)) {
            const keys = Object.keys(expression);
            if (keys.length === 1 && keys[0].startsWith('$')) {
                return this.evaluateOperator(keys[0], expression[keys[0]], document);
            }
            
            // Otherwise, evaluate each property
            const result = {};
            for (const [key, value] of Object.entries(expression)) {
                result[key] = this.evaluate(value, document);
            }
            return result;
        }
        
        // Array - evaluate each element
        if (Array.isArray(expression)) {
            return expression.map(item => this.evaluate(item, document));
        }
        
        // Literal value
        return expression;
    }
    
    evaluateOperator(operator, operands, document) {
        switch (operator) {
            // Arithmetic
            case '$add':
                return operands.reduce((sum, op) => 
                    sum + this.evaluate(op, document), 0);
            case '$subtract':
                const [minuend, subtrahend] = operands;
                return this.evaluate(minuend, document) - this.evaluate(subtrahend, document);
            case '$multiply':
                return operands.reduce((product, op) => 
                    product * this.evaluate(op, document), 1);
            case '$divide':
                const [dividend, divisor] = operands;
                return this.evaluate(dividend, document) / this.evaluate(divisor, document);
            case '$mod':
                const [n, m] = operands;
                return this.evaluate(n, document) % this.evaluate(m, document);
                
            // String
            case '$concat':
                return operands.map(op => 
                    String(this.evaluate(op, document))
                ).join('');
            case '$substr':
                const [str, start, length] = operands;
                return String(this.evaluate(str, document)).substr(
                    this.evaluate(start, document),
                    this.evaluate(length, document)
                );
            case '$toLower':
                return String(this.evaluate(operands, document)).toLowerCase();
            case '$toUpper':
                return String(this.evaluate(operands, document)).toUpperCase();
                
            // Comparison
            case '$eq':
                const [a, b] = operands;
                return this.evaluate(a, document) === this.evaluate(b, document);
            case '$ne':
                return this.evaluate(operands[0], document) !== this.evaluate(operands[1], document);
            case '$gt':
                return this.evaluate(operands[0], document) > this.evaluate(operands[1], document);
            case '$gte':
                return this.evaluate(operands[0], document) >= this.evaluate(operands[1], document);
            case '$lt':
                return this.evaluate(operands[0], document) < this.evaluate(operands[1], document);
            case '$lte':
                return this.evaluate(operands[0], document) <= this.evaluate(operands[1], document);
                
            // Logical
            case '$and':
                return operands.every(op => this.evaluate(op, document));
            case '$or':
                return operands.some(op => this.evaluate(op, document));
            case '$not':
                return !this.evaluate(operands, document);
                
            // Conditional
            case '$cond':
                const { if: condition, then: thenBranch, else: elseBranch } = operands;
                return this.evaluate(condition, document) 
                    ? this.evaluate(thenBranch, document)
                    : this.evaluate(elseBranch, document);
            case '$ifNull':
                const [expr, replacement] = operands;
                const val = this.evaluate(expr, document);
                return val === null || val === undefined 
                    ? this.evaluate(replacement, document)
                    : val;
                    
            // Array
            case '$size':
                const arr = this.evaluate(operands, document);
                return Array.isArray(arr) ? arr.length : 0;
            case '$arrayElemAt':
                const [array, index] = operands;
                const arrVal = this.evaluate(array, document);
                const idx = this.evaluate(index, document);
                return Array.isArray(arrVal) ? arrVal[idx] : undefined;
                
            // Date
            case '$year':
            case '$month':
            case '$dayOfMonth':
            case '$hour':
            case '$minute':
            case '$second':
                const date = new Date(this.evaluate(operands, document));
                switch (operator) {
                    case '$year': return date.getFullYear();
                    case '$month': return date.getMonth() + 1;
                    case '$dayOfMonth': return date.getDate();
                    case '$hour': return date.getHours();
                    case '$minute': return date.getMinutes();
                    case '$second': return date.getSeconds();
                }
                break;
                
            default:
                throw new Error(`Unknown operator: ${operator}`);
        }
    }
    
    getFieldValue(doc, path) {
        const parts = path.split('.');
        let value = doc;
        
        for (const part of parts) {
            if (value == null) return undefined;
            value = value[part];
        }
        
        return value;
    }
}

export default AggregationPipeline;