/**
 * Model Trainer Module
 * Orchestrates the model training process using DataFlood components
 */

import { PromptAnalyzer } from './prompt-analyzer.js';
import { SampleGenerator } from './sample-generator.js';
import { SchemaInferrer } from '../dataflood-js/schema/inferrer.js';
import { IncrementalTrainer as DataFloodTrainer } from '../dataflood-js/training/incremental-trainer.js';
import { DataFloodStorage } from '../welldb-node/storage/dataflood-storage.js';
import logger from '../utils/logger.js';
import config from '../config/config-loader.js';

const log = logger.child('ModelTrainer');

export class ModelTrainer {
    constructor(options = {}) {
        this.promptAnalyzer = new PromptAnalyzer();
        this.sampleGenerator = new SampleGenerator();
        this.schemaInferrer = new SchemaInferrer();
        this.datafloodTrainer = new DataFloodTrainer();
        
        // Initialize storage if path provided
        if (options.storagePath) {
            this.storage = new DataFloodStorage({ basePath: options.storagePath });
        }
        
        this.options = {
            defaultSampleCount: 100,
            iterativeRefinement: true,
            maxIterations: 3,
            validationSplit: 0.2,
            ...options
        };
    }
    
    /**
     * Train a model from a natural language prompt
     */
    async trainFromPrompt(prompt, modelName, options = {}) {
        const config = { ...this.options, ...options };
        
        log.info(`Training model '${modelName}' from prompt`);
        
        try {
            // Step 1: Generate initial samples from prompt
            const initialResult = await this.sampleGenerator.generateFromPrompt(prompt, {
                count: config.defaultSampleCount
            });
            
            // Step 2: Train model with generated samples
            let model = this.datafloodTrainer.train(initialResult.samples);
            model.title = modelName;
            model.description = `Model trained from prompt: ${prompt}`;
            
            // Step 3: Iterative refinement if enabled
            if (config.iterativeRefinement) {
                model = await this.refineModel(model, initialResult.samples, config);
            }
            
            // Step 4: Save model if storage available
            if (this.storage) {
                await this.storage.saveModel(config.storage.defaultDatabase, modelName, model);
                log.info(`Model '${modelName}' saved to storage`);
            }
            
            // Step 5: Validate model
            const validation = await this.validateModel(model, config.validationSplit);
            
            return {
                model,
                metadata: {
                    modelName,
                    prompt,
                    samplesUsed: initialResult.samples.length,
                    iterations: config.iterativeRefinement ? config.maxIterations : 1,
                    validation
                }
            };
            
        } catch (error) {
            log.error(`Failed to train model '${modelName}':`, error);
            throw error;
        }
    }
    
    /**
     * Train a model from sample documents
     */
    async trainFromSamples(samples, modelName, options = {}) {
        const config = { ...this.options, ...options };
        
        log.info(`Training model '${modelName}' from ${samples.length} samples`);
        
        try {
            // Step 1: Split samples for training and validation
            const { trainSet, validationSet } = this.splitSamples(samples, config.validationSplit);
            
            // Step 2: Train model
            let model = this.datafloodTrainer.train(trainSet);
            model.title = modelName;
            model.description = `Model trained from ${trainSet.length} samples`;
            
            // Step 3: Iterative refinement if enabled
            if (config.iterativeRefinement) {
                model = await this.refineModel(model, trainSet, config);
            }
            
            // Step 4: Save model if storage available
            if (this.storage) {
                await this.storage.saveModel(config.storage.defaultDatabase, modelName, model);
                log.info(`Model '${modelName}' saved to storage`);
            }
            
            // Step 5: Validate with validation set
            const validation = this.validateWithSamples(model, validationSet);
            
            return {
                model,
                metadata: {
                    modelName,
                    totalSamples: samples.length,
                    trainingSamples: trainSet.length,
                    validationSamples: validationSet.length,
                    iterations: config.iterativeRefinement ? config.maxIterations : 1,
                    validation
                }
            };
            
        } catch (error) {
            log.error(`Failed to train model '${modelName}':`, error);
            throw error;
        }
    }
    
    /**
     * Incrementally train an existing model
     */
    async incrementalTrain(modelName, newSamples, options = {}) {
        const config = { ...this.options, ...options };
        
        log.info(`Incremental training for model '${modelName}' with ${newSamples.length} new samples`);
        
        try {
            // Step 1: Load existing model
            let model = null;
            if (this.storage) {
                model = await this.storage.getModel(config.storage.defaultDatabase, modelName);
            }
            
            if (!model) {
                throw new Error(`Model '${modelName}' not found`);
            }
            
            // Step 2: Perform incremental training
            model = this.datafloodTrainer.train(newSamples, model);
            
            // Step 3: Update model metadata
            model.description = `${model.description} | Updated with ${newSamples.length} new samples`;
            
            // Step 4: Save updated model
            if (this.storage) {
                await this.storage.saveModel('trained', modelName, model);
                log.info(`Model '${modelName}' updated in storage`);
            }
            
            // Step 5: Validate updated model
            const validation = await this.validateModel(model, config.validationSplit);
            
            return {
                model,
                metadata: {
                    modelName,
                    newSamples: newSamples.length,
                    incremental: true,
                    validation
                }
            };
            
        } catch (error) {
            log.error(`Failed to incrementally train model '${modelName}':`, error);
            throw error;
        }
    }
    
    /**
     * Train multiple related models
     */
    async trainModelFamily(familyName, prompts, options = {}) {
        const config = { ...this.options, ...options };
        const models = {};
        const results = [];
        
        log.info(`Training model family '${familyName}' with ${prompts.length} models`);
        
        for (const [index, promptConfig] of prompts.entries()) {
            const { name, prompt, samples } = 
                typeof promptConfig === 'string' 
                    ? { name: `${familyName}_${index}`, prompt: promptConfig, samples: null }
                    : promptConfig;
            
            try {
                let result;
                if (samples) {
                    result = await this.trainFromSamples(samples, name, config);
                } else {
                    result = await this.trainFromPrompt(prompt, name, config);
                }
                
                models[name] = result.model;
                results.push({
                    name,
                    success: true,
                    metadata: result.metadata
                });
                
            } catch (error) {
                log.error(`Failed to train model '${name}':`, error);
                results.push({
                    name,
                    success: false,
                    error: error.message
                });
            }
        }
        
        return {
            family: familyName,
            models,
            results,
            summary: {
                total: prompts.length,
                successful: results.filter(r => r.success).length,
                failed: results.filter(r => !r.success).length
            }
        };
    }
    
    /**
     * Refine a model through iterative training
     */
    async refineModel(model, samples, config) {
        log.debug('Starting model refinement');
        
        for (let i = 0; i < config.maxIterations; i++) {
            // Generate new samples from current model
            const generator = new (await import('../dataflood-js/generator/document-generator.js')).DocumentGenerator();
            const newSamples = generator.generateDocuments(model, samples.length);
            
            // Combine with original samples
            const combinedSamples = [...samples, ...newSamples];
            
            // Retrain with combined dataset
            model = this.datafloodTrainer.train(combinedSamples, model);
            
            log.debug(`Refinement iteration ${i + 1}/${config.maxIterations} complete`);
        }
        
        return model;
    }
    
    /**
     * Validate a model
     */
    async validateModel(model, validationSplit) {
        const generator = new (await import('../dataflood-js/generator/document-generator.js')).DocumentGenerator();
        const validationSamples = generator.generateDocuments(model, 100);
        
        // Calculate statistics
        const stats = {
            totalFields: Object.keys(model.properties || {}).length,
            requiredFields: (model.required || []).length,
            generationSuccess: validationSamples.length === 100,
            averageFieldsPerDoc: 0,
            fieldCoverage: {}
        };
        
        // Analyze generated samples
        let totalFields = 0;
        const fieldCounts = {};
        
        for (const sample of validationSamples) {
            const sampleFields = Object.keys(sample);
            totalFields += sampleFields.length;
            
            for (const field of sampleFields) {
                fieldCounts[field] = (fieldCounts[field] || 0) + 1;
            }
        }
        
        stats.averageFieldsPerDoc = totalFields / validationSamples.length;
        
        // Calculate field coverage
        for (const field of Object.keys(model.properties || {})) {
            stats.fieldCoverage[field] = (fieldCounts[field] || 0) / validationSamples.length;
        }
        
        return stats;
    }
    
    /**
     * Validate model with specific samples
     */
    validateWithSamples(model, samples) {
        const validator = new (this.sampleGenerator.constructor)();
        const validation = validator.validateSamples(samples, model);
        
        return {
            validationRate: validation.validationRate,
            validSamples: validation.totalValid,
            invalidSamples: validation.totalInvalid,
            errors: validation.invalid.slice(0, 5).map(item => item.errors) // First 5 errors
        };
    }
    
    /**
     * Split samples into training and validation sets
     */
    splitSamples(samples, validationSplit) {
        const shuffled = [...samples].sort(() => Math.random() - 0.5);
        const splitIndex = Math.floor(shuffled.length * (1 - validationSplit));
        
        return {
            trainSet: shuffled.slice(0, splitIndex),
            validationSet: shuffled.slice(splitIndex)
        };
    }
    
    /**
     * Analyze model quality
     */
    analyzeModelQuality(model) {
        const analysis = {
            complexity: 0,
            coverage: 0,
            specificity: 0,
            completeness: 0
        };
        
        // Complexity: number of properties and nested structures
        const properties = model.properties || {};
        analysis.complexity = Object.keys(properties).length;
        
        for (const prop of Object.values(properties)) {
            if (prop.type === 'object' || prop.type === 'array') {
                analysis.complexity += 2;
            }
            if (prop.histogram || prop.stringModel) {
                analysis.complexity += 1;
            }
        }
        
        // Coverage: how many fields have detailed models
        let detailedFields = 0;
        for (const prop of Object.values(properties)) {
            if (prop.histogram || prop.stringModel || prop.enum || prop.pattern) {
                detailedFields++;
            }
        }
        analysis.coverage = Object.keys(properties).length > 0 
            ? detailedFields / Object.keys(properties).length 
            : 0;
        
        // Specificity: presence of constraints
        let constraints = 0;
        for (const prop of Object.values(properties)) {
            if (prop.minimum !== undefined || prop.maximum !== undefined) constraints++;
            if (prop.minLength !== undefined || prop.maxLength !== undefined) constraints++;
            if (prop.pattern || prop.format) constraints++;
            if (prop.enum) constraints++;
        }
        analysis.specificity = Object.keys(properties).length > 0
            ? constraints / Object.keys(properties).length
            : 0;
        
        // Completeness: required fields and descriptions
        analysis.completeness = 0;
        if (model.title) analysis.completeness += 0.25;
        if (model.description) analysis.completeness += 0.25;
        if (model.required && model.required.length > 0) analysis.completeness += 0.25;
        if (Object.keys(properties).length > 3) analysis.completeness += 0.25;
        
        // Overall quality score
        analysis.overall = (
            analysis.complexity * 0.2 +
            analysis.coverage * 0.3 +
            analysis.specificity * 0.3 +
            analysis.completeness * 0.2
        ) / 10; // Normalize to 0-1
        
        return analysis;
    }
    
    /**
     * Get training recommendations
     */
    getTrainingRecommendations(model, samples = []) {
        const recommendations = [];
        const analysis = this.analyzeModelQuality(model);
        
        if (analysis.coverage < 0.5) {
            recommendations.push({
                type: 'coverage',
                message: 'Model lacks detailed field models. Consider providing more diverse samples.',
                priority: 'high'
            });
        }
        
        if (analysis.specificity < 0.3) {
            recommendations.push({
                type: 'specificity',
                message: 'Model lacks constraints. Add minimum/maximum values and patterns.',
                priority: 'medium'
            });
        }
        
        if (!model.required || model.required.length === 0) {
            recommendations.push({
                type: 'required',
                message: 'No required fields defined. Mark essential fields as required.',
                priority: 'medium'
            });
        }
        
        if (samples.length < 50) {
            recommendations.push({
                type: 'samples',
                message: `Only ${samples.length} samples provided. Recommend at least 50 for better training.`,
                priority: 'high'
            });
        }
        
        const properties = model.properties || {};
        if (Object.keys(properties).length < 3) {
            recommendations.push({
                type: 'fields',
                message: 'Model has very few fields. Consider adding more properties.',
                priority: 'low'
            });
        }
        
        return recommendations;
    }
}

export default ModelTrainer;