/**
 * Tide Service Module
 * Port of DataFlood C# TideService.cs
 * Manages time-based document generation sequences
 */

import { DocumentGenerator } from '../generator/document-generator.js';
import { DataFloodModel } from '../models/DataFloodModel.js';
import { readFileSync, existsSync } from 'fs';
import path from 'path';
import logger from '../../utils/logger.js';

const log = logger.child('TideService');

/**
 * TideService manages time-based document generation
 */
export class TideService {
  constructor() {
    this.schemaCache = new Map();
    this.random = Math.random;
  }
  
  /**
   * Execute a tide configuration
   * @param {Object} request - TideExecutionRequest
   * @returns {Promise<Object>} TideResponse
   */
  async executeTide(request) {
    const executionStart = new Date();
    
    try {
      // Load or validate configuration
      const config = await this.loadConfiguration(request);
      if (!config) {
        return {
          success: false,
          message: 'Failed to load configuration'
        };
      }
      
      // Validate configuration
      const validation = await this.validateConfiguration(config);
      if (!validation.isValid) {
        return {
          success: false,
          message: `Configuration validation failed: ${validation.errors.join(', ')}`
        };
      }
      
      if (request.validateOnly) {
        return {
          success: true,
          message: 'Configuration is valid',
          metadata: this.createMetadata(config, executionStart, new Date(), [])
        };
      }
      
      // Apply overrides
      this.applyRequestOverrides(config, request);
      
      // Initialize random seed if provided
      if (config.seed !== null && config.seed !== undefined) {
        this.random = this.seededRandom(config.seed);
      }
      
      // Execute the sequence
      const documents = await this.generateSequenceDocuments(config, request.maxDocuments);
      
      const executionEnd = new Date();
      
      return {
        success: true,
        message: `Generated ${documents.length} documents`,
        totalDocuments: documents.length,
        generationTime: executionEnd - executionStart,
        documents: request.returnDocuments ? documents : [],
        metadata: this.createMetadata(config, executionStart, executionEnd, documents)
      };
    } catch (error) {
      log.error('Tide execution failed', error);
      return {
        success: false,
        message: `Execution failed: ${error.message}`,
        generationTime: new Date() - executionStart
      };
    }
  }
  
  /**
   * Validate a tide configuration
   * @param {Object} configuration - TideConfig
   * @returns {Promise<Object>} Validation result
   */
  async validateConfiguration(configuration) {
    const result = { isValid: true, errors: [] };
    
    try {
      // Basic validation
      if (!configuration.startTime || !configuration.endTime) {
        result.errors.push('Start time and end time are required');
        result.isValid = false;
      } else {
        const startTime = new Date(configuration.startTime);
        const endTime = new Date(configuration.endTime);
        
        if (endTime <= startTime) {
          result.errors.push('End time must be after start time');
          result.isValid = false;
        }
      }
      
      if (!configuration.intervalMs || configuration.intervalMs <= 0) {
        result.errors.push('Interval must be positive');
        result.isValid = false;
      }
      
      if (!configuration.steps || configuration.steps.length === 0) {
        result.errors.push('At least one sequence step is required');
        result.isValid = false;
      }
      
      // Validate steps
      const stepIds = new Set();
      for (const step of configuration.steps || []) {
        if (stepIds.has(step.stepId)) {
          result.errors.push(`Duplicate step ID: ${step.stepId}`);
          result.isValid = false;
        }
        stepIds.add(step.stepId);
        
        // Check if model file exists (if path is provided)
        if (step.modelPath) {
          const modelPath = this.resolveModelPath(step.modelPath);
          if (!existsSync(modelPath)) {
            result.errors.push(`Model file not found: ${step.modelPath}`);
            result.isValid = false;
          } else {
            // Try to load and validate the schema
            try {
              await this.loadSchema(modelPath);
            } catch (error) {
              result.errors.push(`Invalid schema file ${step.modelPath}: ${error.message}`);
              result.isValid = false;
            }
          }
        }
        
        // Validate probability
        if (step.generationProbability !== undefined && 
            (step.generationProbability < 0 || step.generationProbability > 1)) {
          result.errors.push(`Step ${step.stepId}: generationProbability must be between 0 and 1`);
          result.isValid = false;
        }
        
        // Validate documentsPerInterval
        if (step.documentsPerInterval !== undefined && step.documentsPerInterval <= 0) {
          result.errors.push(`Step ${step.stepId}: documentsPerInterval must be positive`);
          result.isValid = false;
        }
      }
      
    } catch (error) {
      result.errors.push(`Validation error: ${error.message}`);
      result.isValid = false;
    }
    
    return result;
  }
  
  /**
   * Get statistics for a tide configuration
   * @param {Object} configuration - TideConfig
   * @returns {Promise<Object>} Statistics
   */
  async getTideStatistics(configuration) {
    const startTime = new Date(configuration.startTime);
    const endTime = new Date(configuration.endTime);
    const totalDuration = endTime - startTime;
    
    const stats = {
      totalDuration: totalDuration,
      totalSteps: configuration.steps.length,
      totalTransactions: (configuration.transactions || []).length,
      modelsUsed: [...new Set(configuration.steps.map(s => s.modelPath))],
      estimatedDocuments: 0,
      stepDurations: {}
    };
    
    // Calculate estimated documents for absolute time tide
    const totalIntervals = Math.floor(totalDuration / configuration.intervalMs);
    
    for (const step of configuration.steps) {
      const stepStart = new Date(step.startTime);
      const stepEnd = step.endTime ? new Date(step.endTime) : endTime;
      
      // Calculate step duration
      const stepDuration = Math.min(stepEnd, endTime) - Math.max(stepStart, startTime);
      if (stepDuration > 0) {
        const stepIntervals = Math.floor(stepDuration / configuration.intervalMs);
        const docsPerInterval = step.documentsPerInterval || 1;
        const probability = step.generationProbability || 1;
        
        stats.estimatedDocuments += Math.floor(stepIntervals * docsPerInterval * probability);
        stats.stepDurations[step.stepId] = stepDuration;
      }
    }
    
    return stats;
  }
  
  /**
   * Load configuration from request
   */
  async loadConfiguration(request) {
    if (request.configuration) {
      return request.configuration;
    }
    
    if (request.configurationFile) {
      return await this.loadConfigurationFromFile(request.configurationFile);
    }
    
    return null;
  }
  
  /**
   * Load configuration from file
   */
  async loadConfigurationFromFile(configurationFile) {
    if (!existsSync(configurationFile)) {
      throw new Error(`Configuration file not found: ${configurationFile}`);
    }
    
    const jsonContent = readFileSync(configurationFile, 'utf8');
    return JSON.parse(jsonContent);
  }
  
  /**
   * Apply request overrides to configuration
   */
  applyRequestOverrides(config, request) {
    if (request.outputFormatOverride) {
      config.outputFormat = request.outputFormatOverride;
    }
    
    if (request.seedOverride !== null && request.seedOverride !== undefined) {
      config.seed = request.seedOverride;
    }
  }
  
  /**
   * Generate documents according to sequence configuration
   */
  async generateSequenceDocuments(config, maxDocuments) {
    const documents = [];
    const startTime = new Date(config.startTime);
    const endTime = new Date(config.endTime);
    let currentTime = new Date(startTime);
    
    // Initialize document generators for each step
    const generators = new Map();
    for (const step of config.steps) {
      const modelPath = this.resolveModelPath(step.modelPath);
      const schema = await this.loadSchema(modelPath);
      
      // Create generator with seed and entropy override
      let seed = config.seed;
      if (seed !== null && seed !== undefined) {
        // Vary seed per step to avoid identical documents
        seed = seed + step.stepId.charCodeAt(0);
      }
      
      const entropyOverride = step.entropyOverride || config.globalEntropyOverride || null;
      const generator = new DocumentGenerator(seed, entropyOverride);
      
      generators.set(step.stepId, { generator, schema, step });
    }
    
    // Generate documents at each interval
    while (currentTime < endTime) {
      if (maxDocuments && documents.length >= maxDocuments) {
        break;
      }
      
      // Apply jitter if configured
      let intervalMs = config.intervalMs;
      if (config.addJitter) {
        // Add ±10% random variation
        const jitter = (this.random() - 0.5) * 0.2 * intervalMs;
        intervalMs = Math.round(intervalMs + jitter);
      }
      
      // Find active steps for current time
      const activeSteps = this.getActiveSteps(config, currentTime);
      
      // Generate documents for active steps
      for (const step of activeSteps) {
        // Check generation probability
        if (this.random() > (step.generationProbability || 1)) {
          continue;
        }
        
        const { generator, schema } = generators.get(step.stepId);
        const docsPerInterval = step.documentsPerInterval || 1;
        
        for (let i = 0; i < docsPerInterval; i++) {
          if (maxDocuments && documents.length >= maxDocuments) {
            break;
          }
          
          const doc = generator.generateDocument(schema);
          
          // Add metadata if requested
          const tideDocument = {
            timestamp: new Date(currentTime).toISOString(),
            stepId: step.stepId,
            document: doc
          };
          
          if (config.includeMetadata) {
            tideDocument.metadata = {
              modelPath: step.modelPath,
              tags: step.tags || [],
              customProperties: step.customProperties || {}
            };
          }
          
          documents.push(tideDocument);
        }
      }
      
      // Advance time
      currentTime = new Date(currentTime.getTime() + intervalMs);
    }
    
    log.info(`Generated ${documents.length} documents from ${config.steps.length} steps`);
    return documents;
  }
  
  /**
   * Get active steps for a given time
   */
  getActiveSteps(config, currentTime) {
    const activeSteps = [];
    
    for (const step of config.steps) {
      const stepStart = new Date(step.startTime);
      const stepEnd = step.endTime ? new Date(step.endTime) : new Date(config.endTime);
      
      if (currentTime >= stepStart && currentTime < stepEnd) {
        activeSteps.push(step);
      }
    }
    
    // If multiple steps are active, use weights to select
    if (activeSteps.length > 1) {
      return [this.selectByWeight(activeSteps)];
    }
    
    return activeSteps;
  }
  
  /**
   * Select a step based on weights
   */
  selectByWeight(steps) {
    const totalWeight = steps.reduce((sum, step) => sum + (step.weight || 1), 0);
    const randomValue = this.random() * totalWeight;
    
    let currentWeight = 0;
    for (const step of steps) {
      currentWeight += (step.weight || 1);
      if (randomValue < currentWeight) {
        return step;
      }
    }
    
    return steps[0];
  }
  
  /**
   * Load schema from file with caching
   */
  async loadSchema(modelPath) {
    if (this.schemaCache.has(modelPath)) {
      return this.schemaCache.get(modelPath);
    }
    
    const modelData = JSON.parse(readFileSync(modelPath, 'utf8'));
    const schema = new DataFloodModel(modelData);
    
    this.schemaCache.set(modelPath, schema);
    return schema;
  }
  
  /**
   * Resolve model path relative to Test_Data
   */
  resolveModelPath(modelPath) {
    // If absolute path, use as-is
    if (path.isAbsolute(modelPath)) {
      return modelPath;
    }
    
    // Try relative to current directory
    if (existsSync(modelPath)) {
      return modelPath;
    }
    
    // Try relative to Test_Data
    const testDataPath = path.join(process.cwd(), '..', 'Test_Data', modelPath);
    if (existsSync(testDataPath)) {
      return testDataPath;
    }
    
    // Return original path (will fail validation if not found)
    return modelPath;
  }
  
  /**
   * Create metadata for response
   */
  createMetadata(config, startTime, endTime, documents) {
    return {
      configName: config.name,
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      duration: endTime - startTime,
      documentsGenerated: documents.length,
      stepsExecuted: config.steps.length,
      intervalMs: config.intervalMs,
      seed: config.seed
    };
  }
  
  /**
   * Create a seeded random number generator
   */
  seededRandom(seed) {
    let s = seed;
    return function() {
      s = Math.sin(s) * 10000;
      return s - Math.floor(s);
    };
  }
}

/**
 * TideConfig class for configuration validation
 */
export class TideConfig {
  constructor(data = {}) {
    this.name = data.name || 'New Sequence';
    this.description = data.description || '';
    this.startTime = data.startTime || new Date().toISOString();
    this.endTime = data.endTime || new Date(Date.now() + 3600000).toISOString(); // +1 hour
    this.intervalMs = data.intervalMs || 1000;
    this.seed = data.seed || null;
    this.addJitter = data.addJitter || false;
    this.outputFormat = data.outputFormat || 'json';
    this.includeMetadata = data.includeMetadata || false;
    this.steps = data.steps || [];
    this.transactions = data.transactions || [];
    this.globalEntropyOverride = data.globalEntropyOverride || null;
  }
}

/**
 * TideStep class for step configuration
 */
export class TideStep {
  constructor(data = {}) {
    this.stepId = data.stepId || '';
    this.startTime = data.startTime || new Date().toISOString();
    this.endTime = data.endTime || null;
    this.modelPath = data.modelPath || '';
    this.weight = data.weight || 1.0;
    this.documentsPerInterval = data.documentsPerInterval || 1;
    this.generationProbability = data.generationProbability || 1.0;
    this.entropyOverride = data.entropyOverride || null;
    this.tags = data.tags || [];
    this.customProperties = data.customProperties || {};
  }
}