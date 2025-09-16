/**
 * Sample Generator Module
 * Generates sample data from prompts using PromptAnalyzer and DocumentGenerator
 */

import { PromptAnalyzer } from './prompt-analyzer.js';
import { DocumentGenerator } from '../dataflood-js/generator/document-generator.js';
import { SchemaInferrer } from '../dataflood-js/schema/inferrer.js';
import logger from '../utils/logger.js';

const log = logger.child('SampleGenerator');

export class SampleGenerator {
    constructor() {
        this.promptAnalyzer = new PromptAnalyzer();
        this.documentGenerator = new DocumentGenerator();
        this.schemaInferrer = new SchemaInferrer();
    }
    
    /**
     * Generate samples from a natural language prompt
     */
    async generateFromPrompt(prompt, options = {}) {
        const {
            count = 10,
            includeSchema = true,
            inferFromSamples = true
        } = options;
        
        log.debug(`Generating ${count} samples from prompt:`, prompt);
        
        try {
            // Step 1: Analyze the prompt to extract schema
            const analysis = this.promptAnalyzer.analyze(prompt);
            let schema = analysis.schema;
            
            // Step 2: Generate initial samples using the extracted schema
            const initialSamples = this.documentGenerator.generateDocuments(schema, count);
            
            // Step 3: If requested, infer a more detailed schema from the generated samples
            if (inferFromSamples && initialSamples.length > 0) {
                const inferredSchema = this.schemaInferrer.inferSchema(initialSamples);
                
                // Merge the inferred schema with the original to get best of both
                schema = this.mergeSchemas(schema, inferredSchema);
                
                // Generate final samples with the improved schema
                const finalSamples = this.documentGenerator.generateDocuments(schema, count);
                
                log.debug(`Generated ${finalSamples.length} samples with refined schema`);
                
                const result = {
                    samples: finalSamples,
                    metadata: {
                        ...analysis.metadata,
                        samplesGenerated: finalSamples.length,
                        schemaRefined: true
                    }
                };
                
                if (includeSchema) {
                    result.schema = schema;
                }
                
                return result;
            }
            
            log.debug(`Generated ${initialSamples.length} samples`);
            
            const result = {
                samples: initialSamples,
                metadata: {
                    ...analysis.metadata,
                    samplesGenerated: initialSamples.length,
                    schemaRefined: false
                }
            };
            
            if (includeSchema) {
                result.schema = schema;
            }
            
            return result;
            
        } catch (error) {
            log.error('Failed to generate samples from prompt:', error);
            throw error;
        }
    }
    
    /**
     * Generate a single sample from prompt analysis
     * This is used by the MCP server to generate individual documents
     */
    generateFromAnalysis(analysis) {
        try {
            // Use the schema from the analysis to generate a document
            const schema = analysis.schema || analysis;
            const document = this.documentGenerator.generateDocument(schema);
            
            // Add any metadata fields from the analysis
            if (analysis.metadata) {
                if (analysis.metadata.modelName) {
                    document._modelName = analysis.metadata.modelName;
                }
                if (analysis.metadata.collectionName) {
                    document._collection = analysis.metadata.collectionName;
                }
            }
            
            return document;
        } catch (error) {
            log.error('Failed to generate sample from analysis:', error);
            // Return a fallback document if generation fails
            return {
                _id: this.generateObjectId(),
                data: 'Sample data',
                timestamp: new Date().toISOString(),
                error: error.message
            };
        }
    }
    
    /**
     * Generate a MongoDB-style ObjectId
     */
    generateObjectId() {
        const timestamp = Math.floor(Date.now() / 1000).toString(16);
        const random = Math.random().toString(16).substring(2, 18);
        return (timestamp + random).substring(0, 24).padEnd(24, '0');
    }
    
    /**
     * Generate samples from existing sample documents
     */
    async generateFromSamples(samples, options = {}) {
        const {
            count = 10,
            includeSchema = true,
            modelName = 'sample-model'
        } = options;
        
        log.debug(`Generating ${count} samples from ${samples.length} input samples`);
        
        try {
            // Infer schema from samples
            const schema = this.schemaInferrer.inferSchema(samples);
            schema.title = modelName;
            
            // Generate new samples
            const generatedSamples = this.documentGenerator.generateDocuments(schema, count);
            
            log.debug(`Generated ${generatedSamples.length} samples`);
            
            const result = {
                samples: generatedSamples,
                metadata: {
                    inputSamples: samples.length,
                    samplesGenerated: generatedSamples.length,
                    modelName
                }
            };
            
            if (includeSchema) {
                result.schema = schema;
            }
            
            return result;
            
        } catch (error) {
            log.error('Failed to generate samples from input samples:', error);
            throw error;
        }
    }
    
    /**
     * Generate samples with specific constraints
     */
    async generateWithConstraints(basePrompt, constraints, options = {}) {
        const {
            count = 10,
            includeSchema = true
        } = options;
        
        log.debug('Generating samples with constraints:', constraints);
        
        try {
            // Analyze base prompt
            const analysis = this.promptAnalyzer.analyze(basePrompt);
            let schema = analysis.schema;
            
            // Apply constraints to schema
            schema = this.applyConstraints(schema, constraints);
            
            // Generate samples with constrained schema
            const samples = this.documentGenerator.generateDocuments(schema, count);
            
            log.debug(`Generated ${samples.length} constrained samples`);
            
            const result = {
                samples,
                metadata: {
                    ...analysis.metadata,
                    samplesGenerated: samples.length,
                    constraintsApplied: Object.keys(constraints).length
                }
            };
            
            if (includeSchema) {
                result.schema = schema;
            }
            
            return result;
            
        } catch (error) {
            log.error('Failed to generate constrained samples:', error);
            throw error;
        }
    }
    
    /**
     * Generate diverse samples with variations
     */
    async generateDiverseSamples(prompt, options = {}) {
        const {
            batches = 3,
            samplesPerBatch = 5,
            variationSeed = Math.random(),
            includeSchema = true
        } = options;
        
        log.debug(`Generating ${batches} diverse batches of ${samplesPerBatch} samples each`);
        
        try {
            // Analyze prompt
            const analysis = this.promptAnalyzer.analyze(prompt);
            let schema = analysis.schema;
            
            const allSamples = [];
            const variations = [];
            
            // Generate multiple batches with different seeds
            for (let i = 0; i < batches; i++) {
                const seed = variationSeed + i;
                const generator = new DocumentGenerator(seed);
                
                // Slightly vary the schema for each batch
                const variedSchema = this.varySchema(schema, i / batches);
                
                const batchSamples = generator.generateDocuments(variedSchema, samplesPerBatch);
                allSamples.push(...batchSamples);
                
                variations.push({
                    batch: i + 1,
                    seed,
                    samples: batchSamples.length
                });
            }
            
            log.debug(`Generated ${allSamples.length} diverse samples across ${batches} batches`);
            
            const result = {
                samples: allSamples,
                metadata: {
                    ...analysis.metadata,
                    totalSamples: allSamples.length,
                    batches,
                    samplesPerBatch,
                    variations
                }
            };
            
            if (includeSchema) {
                result.schema = schema;
            }
            
            return result;
            
        } catch (error) {
            log.error('Failed to generate diverse samples:', error);
            throw error;
        }
    }
    
    /**
     * Merge two schemas, preferring the more detailed one
     */
    mergeSchemas(schema1, schema2) {
        const merged = {
            ...schema1,
            properties: {}
        };
        
        // Merge properties
        const allProps = new Set([
            ...Object.keys(schema1.properties || {}),
            ...Object.keys(schema2.properties || {})
        ]);
        
        for (const prop of allProps) {
            const prop1 = (schema1.properties || {})[prop];
            const prop2 = (schema2.properties || {})[prop];
            
            if (prop1 && prop2) {
                // Merge both definitions, preferring the more detailed one
                merged.properties[prop] = this.mergePropertySchemas(prop1, prop2);
            } else {
                merged.properties[prop] = prop1 || prop2;
            }
        }
        
        // Merge required arrays
        const required1 = new Set(schema1.required || []);
        const required2 = new Set(schema2.required || []);
        merged.required = Array.from(new Set([...required1, ...required2]));
        
        // Use the more specific title and description
        if (schema2.title && (!schema1.title || schema2.title.length > schema1.title.length)) {
            merged.title = schema2.title;
        }
        if (schema2.description && (!schema1.description || schema2.description.length > schema1.description.length)) {
            merged.description = schema2.description;
        }
        
        return merged;
    }
    
    /**
     * Merge two property schemas
     */
    mergePropertySchemas(prop1, prop2) {
        const merged = { ...prop1 };
        
        // Prefer more specific types
        if (prop2.type && prop2.type !== 'string' && prop1.type === 'string') {
            merged.type = prop2.type;
        }
        
        // Merge constraints
        if (prop2.minimum !== undefined) merged.minimum = prop2.minimum;
        if (prop2.maximum !== undefined) merged.maximum = prop2.maximum;
        if (prop2.minLength !== undefined) merged.minLength = prop2.minLength;
        if (prop2.maxLength !== undefined) merged.maxLength = prop2.maxLength;
        if (prop2.pattern) merged.pattern = prop2.pattern;
        if (prop2.format) merged.format = prop2.format;
        if (prop2.enum) merged.enum = prop2.enum;
        
        // Merge DataFlood extensions
        if (prop2.histogram) merged.histogram = prop2.histogram;
        if (prop2.stringModel) merged.stringModel = prop2.stringModel;
        
        // Use longer description
        if (prop2.description && (!prop1.description || prop2.description.length > prop1.description.length)) {
            merged.description = prop2.description;
        }
        
        return merged;
    }
    
    /**
     * Apply constraints to a schema
     */
    applyConstraints(schema, constraints) {
        const constrained = JSON.parse(JSON.stringify(schema));  // Deep clone
        
        for (const [field, fieldConstraints] of Object.entries(constraints)) {
            if (!constrained.properties[field]) {
                // Add field if it doesn't exist
                constrained.properties[field] = {
                    type: 'string'
                };
            }
            
            const prop = constrained.properties[field];
            
            // Apply each constraint
            for (const [constraint, value] of Object.entries(fieldConstraints)) {
                switch (constraint) {
                    case 'type':
                        prop.type = value;
                        break;
                    case 'min':
                    case 'minimum':
                        prop.minimum = value;
                        break;
                    case 'max':
                    case 'maximum':
                        prop.maximum = value;
                        break;
                    case 'length':
                    case 'maxLength':
                        prop.maxLength = value;
                        break;
                    case 'minLength':
                        prop.minLength = value;
                        break;
                    case 'pattern':
                        prop.pattern = value;
                        break;
                    case 'enum':
                    case 'values':
                        prop.enum = Array.isArray(value) ? value : [value];
                        break;
                    case 'required':
                        if (value && !constrained.required.includes(field)) {
                            constrained.required.push(field);
                        }
                        break;
                    case 'format':
                        prop.format = value;
                        break;
                    default:
                        prop[constraint] = value;
                }
            }
        }
        
        return constrained;
    }
    
    /**
     * Vary a schema slightly for diversity
     */
    varySchema(schema, variation) {
        const varied = JSON.parse(JSON.stringify(schema));  // Deep clone
        
        // Vary numeric constraints slightly
        for (const prop of Object.values(varied.properties || {})) {
            if (prop.type === 'number' || prop.type === 'integer') {
                if (prop.minimum !== undefined) {
                    prop.minimum = prop.minimum * (1 - variation * 0.1);
                }
                if (prop.maximum !== undefined) {
                    prop.maximum = prop.maximum * (1 + variation * 0.1);
                }
            }
            
            if (prop.type === 'string') {
                if (prop.minLength !== undefined && prop.minLength > 1) {
                    prop.minLength = Math.max(1, Math.floor(prop.minLength * (1 - variation * 0.2)));
                }
                if (prop.maxLength !== undefined) {
                    prop.maxLength = Math.ceil(prop.maxLength * (1 + variation * 0.2));
                }
            }
        }
        
        return varied;
    }
    
    /**
     * Validate samples against a schema
     */
    validateSamples(samples, schema) {
        const results = {
            valid: [],
            invalid: [],
            totalValid: 0,
            totalInvalid: 0
        };
        
        for (const sample of samples) {
            const validation = this.validateSample(sample, schema);
            if (validation.valid) {
                results.valid.push(sample);
                results.totalValid++;
            } else {
                results.invalid.push({
                    sample,
                    errors: validation.errors
                });
                results.totalInvalid++;
            }
        }
        
        results.validationRate = results.totalValid / samples.length;
        
        return results;
    }
    
    /**
     * Validate a single sample against schema
     */
    validateSample(sample, schema) {
        const errors = [];
        
        // Check required fields
        if (schema.required) {
            for (const field of schema.required) {
                if (!(field in sample)) {
                    errors.push(`Missing required field: ${field}`);
                }
            }
        }
        
        // Check property types and constraints
        for (const [field, value] of Object.entries(sample)) {
            const propSchema = schema.properties?.[field];
            if (!propSchema) continue;
            
            // Type check
            const actualType = Array.isArray(value) ? 'array' : typeof value;
            if (propSchema.type && actualType !== propSchema.type) {
                errors.push(`Field ${field}: expected type ${propSchema.type}, got ${actualType}`);
            }
            
            // Constraint checks
            if (propSchema.type === 'number' || propSchema.type === 'integer') {
                if (propSchema.minimum !== undefined && value < propSchema.minimum) {
                    errors.push(`Field ${field}: value ${value} below minimum ${propSchema.minimum}`);
                }
                if (propSchema.maximum !== undefined && value > propSchema.maximum) {
                    errors.push(`Field ${field}: value ${value} above maximum ${propSchema.maximum}`);
                }
            }
            
            if (propSchema.type === 'string') {
                if (propSchema.minLength !== undefined && value.length < propSchema.minLength) {
                    errors.push(`Field ${field}: length ${value.length} below minimum ${propSchema.minLength}`);
                }
                if (propSchema.maxLength !== undefined && value.length > propSchema.maxLength) {
                    errors.push(`Field ${field}: length ${value.length} above maximum ${propSchema.maxLength}`);
                }
                if (propSchema.pattern) {
                    const regex = new RegExp(propSchema.pattern);
                    if (!regex.test(value)) {
                        errors.push(`Field ${field}: value doesn't match pattern ${propSchema.pattern}`);
                    }
                }
            }
            
            if (propSchema.enum && !propSchema.enum.includes(value)) {
                errors.push(`Field ${field}: value ${value} not in enum ${propSchema.enum}`);
            }
        }
        
        return {
            valid: errors.length === 0,
            errors
        };
    }
}

export default SampleGenerator;