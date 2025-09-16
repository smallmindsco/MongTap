/**
 * Incremental Training Module
 * Supports updating existing models with new data cumulatively
 * Matches DataFlood's MergeSchemas approach
 */

import inferrer from '../schema/inferrer.js';

class IncrementalTrainer {
    constructor() {
        this.inferrer = inferrer;
    }
    
    /**
     * Update an existing model with new data
     * @param {Object} existingModel - The current model
     * @param {Array} newData - New documents to learn from
     * @returns {Object} Updated model combining existing and new knowledge
     */
    updateModel(existingModel, newData) {
        if (!newData || newData.length === 0) {
            return existingModel;
        }
        
        // Infer schema from new data
        const newModel = this.inferrer.inferSchema(newData);
        
        // Merge the models
        return this.mergeSchemas(existingModel, newModel);
    }
    
    /**
     * Merge two schemas together, combining their properties and statistics
     * Port of DataFlood's MergeSchemas method
     */
    mergeSchemas(existing, newSchema) {
        if (!existing) return newSchema;
        if (!newSchema) return existing;
        
        const merged = {
            $schema: existing.$schema || newSchema.$schema,
            type: this.mergeTypes(existing.type, newSchema.type)
        };
        
        // Merge string constraints and models
        if (existing.type === 'string' || newSchema.type === 'string') {
            merged.minLength = Math.min(
                existing.minLength ?? Number.MAX_SAFE_INTEGER,
                newSchema.minLength ?? Number.MAX_SAFE_INTEGER
            );
            merged.maxLength = Math.max(
                existing.maxLength ?? 0,
                newSchema.maxLength ?? 0
            );
            
            if (merged.minLength === Number.MAX_SAFE_INTEGER) delete merged.minLength;
            if (merged.maxLength === 0) delete merged.maxLength;
            
            // Merge string models
            merged.stringModel = this.mergeStringModels(existing.stringModel, newSchema.stringModel);
            
            // Merge formats
            if (existing.format || newSchema.format) {
                merged.format = existing.format || newSchema.format;
            }
            
            // Merge patterns
            if (existing.pattern || newSchema.pattern) {
                merged.pattern = existing.pattern || newSchema.pattern;
            }
        }
        
        // Merge number/integer constraints
        if (['number', 'integer'].includes(existing.type) || 
            ['number', 'integer'].includes(newSchema.type)) {
            
            if (existing.minimum !== undefined && newSchema.minimum !== undefined) {
                merged.minimum = Math.min(existing.minimum, newSchema.minimum);
            } else {
                merged.minimum = existing.minimum ?? newSchema.minimum;
            }
            
            if (existing.maximum !== undefined && newSchema.maximum !== undefined) {
                merged.maximum = Math.max(existing.maximum, newSchema.maximum);
            } else {
                merged.maximum = existing.maximum ?? newSchema.maximum;
            }
            
            // Merge histograms
            merged.histogram = this.mergeHistograms(existing.histogram, newSchema.histogram);
            
            // Merge multipleOf
            if (existing.multipleOf || newSchema.multipleOf) {
                merged.multipleOf = existing.multipleOf || newSchema.multipleOf;
            }
        }
        
        // Merge array constraints
        if (existing.type === 'array' || newSchema.type === 'array') {
            merged.minItems = Math.min(
                existing.minItems ?? Number.MAX_SAFE_INTEGER,
                newSchema.minItems ?? Number.MAX_SAFE_INTEGER
            );
            merged.maxItems = Math.max(
                existing.maxItems ?? 0,
                newSchema.maxItems ?? 0
            );
            
            if (merged.minItems === Number.MAX_SAFE_INTEGER) delete merged.minItems;
            if (merged.maxItems === 0) delete merged.maxItems;
            
            // Merge array item schemas
            if (existing.items || newSchema.items) {
                if (existing.items && newSchema.items) {
                    merged.items = this.mergeSchemas(existing.items, newSchema.items);
                } else {
                    merged.items = existing.items || newSchema.items;
                }
            }
        }
        
        // Merge object properties
        if (existing.type === 'object' || newSchema.type === 'object') {
            merged.properties = this.mergeProperties(
                existing.properties,
                newSchema.properties
            );
            merged.required = this.mergeRequired(
                existing.required,
                newSchema.required,
                merged.properties
            );
        }
        
        // Merge enum values
        if (existing.enum || newSchema.enum) {
            merged.enum = this.mergeEnumValues(existing.enum, newSchema.enum);
        }
        
        // Merge anyOf unions
        if (existing.anyOf || newSchema.anyOf) {
            merged.anyOf = this.mergeAnyOf(existing.anyOf, newSchema.anyOf);
        }
        
        return merged;
    }
    
    /**
     * Merge type information
     */
    mergeTypes(type1, type2) {
        if (!type1) return type2;
        if (!type2) return type1;
        if (type1 === type2) return type1;
        
        // If types differ, create a union (simplified for now)
        // In production, might want to use anyOf
        return type2; // Use newer type
    }
    
    /**
     * Merge object properties
     */
    mergeProperties(props1, props2) {
        if (!props1) return props2;
        if (!props2) return props1;
        
        const merged = { ...props1 };
        
        for (const [key, value] of Object.entries(props2)) {
            if (merged[key]) {
                merged[key] = this.mergeSchemas(merged[key], value);
            } else {
                merged[key] = value;
            }
        }
        
        return merged;
    }
    
    /**
     * Merge required field lists
     */
    mergeRequired(req1, req2, properties) {
        if (!req1 && !req2) return undefined;
        
        const required = new Set();
        
        // Only mark as required if required in both schemas
        if (req1 && req2) {
            for (const field of req1) {
                if (req2.includes(field) && properties && properties[field]) {
                    required.add(field);
                }
            }
        }
        
        return required.size > 0 ? Array.from(required) : undefined;
    }
    
    /**
     * Merge string models with proper statistics combination
     */
    mergeStringModels(model1, model2) {
        if (!model1) return model2;
        if (!model2) return model1;
        
        const merged = {
            minLength: Math.min(model1.minLength, model2.minLength),
            maxLength: Math.max(model1.maxLength, model2.maxLength),
            averageLength: (model1.averageLength + model2.averageLength) / 2
        };
        
        // Merge character frequency
        merged.characterFrequency = this.mergeFrequencies(
            model1.characterFrequency,
            model2.characterFrequency
        );
        
        // Merge unique characters
        const allChars = new Set([
            ...(model1.uniqueCharacters || ''),
            ...(model2.uniqueCharacters || '')
        ]);
        merged.uniqueCharacters = Array.from(allChars).join('');
        
        // Merge value frequency
        merged.valueFrequency = this.mergeFrequencies(
            model1.valueFrequency,
            model2.valueFrequency
        );
        
        // Merge patterns
        if (model1.patterns || model2.patterns) {
            const patternMap = {};
            
            const addPatterns = (patterns) => {
                if (patterns && Array.isArray(patterns)) {
                    for (const p of patterns) {
                        const key = p.pattern || p;
                        patternMap[key] = (patternMap[key] || 0) + (p.count || 1);
                    }
                } else if (patterns && typeof patterns === 'object') {
                    // Handle object format
                    for (const [pattern, count] of Object.entries(patterns)) {
                        patternMap[pattern] = (patternMap[pattern] || 0) + count;
                    }
                }
            };
            
            addPatterns(model1.patterns);
            addPatterns(model2.patterns);
            
            merged.patterns = Object.entries(patternMap)
                .map(([pattern, count]) => ({ pattern, count }))
                .sort((a, b) => b.count - a.count)
                .slice(0, 10); // Keep top 10 patterns
        }
        
        // Merge n-grams
        if (model1.nGrams || model2.nGrams) {
            merged.nGrams = {};
            for (const n of [2, 3]) {
                const grams1 = model1.nGrams?.[n] || {};
                const grams2 = model2.nGrams?.[n] || {};
                merged.nGrams[n] = this.mergeFrequencies(grams1, grams2);
            }
        }
        
        // Update complexity and entropy
        merged.complexity = Math.max(
            model1.complexity || 0,
            model2.complexity || 0
        );
        merged.entropyScore = (
            (model1.entropyScore || 0) + (model2.entropyScore || 0)
        ) / 2;
        
        // Keep sample values from both
        if (model1.sampleValues || model2.sampleValues) {
            const allSamples = [
                ...(model1.sampleValues || []),
                ...(model2.sampleValues || [])
            ];
            // Keep unique samples, up to 20
            merged.sampleValues = [...new Set(allSamples)].slice(0, 20);
        }
        
        return merged;
    }
    
    /**
     * Merge histograms with proper bin combination
     */
    mergeHistograms(hist1, hist2) {
        if (!hist1) return hist2;
        if (!hist2) return hist1;
        
        const merged = {
            minValue: Math.min(hist1.minValue, hist2.minValue),
            maxValue: Math.max(hist1.maxValue, hist2.maxValue),
            totalCount: (hist1.totalCount || 0) + (hist2.totalCount || 0)
        };
        
        // Merge bins - simplified approach
        // In production, would need to recompute bins based on combined data
        const allBins = [...(hist1.bins || []), ...(hist2.bins || [])];
        
        // Sort and merge overlapping bins
        allBins.sort((a, b) => a.min - b.min);
        
        const mergedBins = [];
        let currentBin = null;
        
        for (const bin of allBins) {
            if (!currentBin) {
                currentBin = { ...bin };
            } else if (bin.min <= currentBin.max) {
                // Bins overlap, merge them
                currentBin.max = Math.max(currentBin.max, bin.max);
                currentBin.count = (currentBin.count || 0) + (bin.count || 0);
                currentBin.frequency = (currentBin.frequency || 0) + (bin.frequency || 0);
            } else {
                // No overlap, save current and start new
                mergedBins.push(currentBin);
                currentBin = { ...bin };
            }
        }
        
        if (currentBin) {
            mergedBins.push(currentBin);
        }
        
        merged.bins = mergedBins.slice(0, 20); // Limit to 20 bins
        
        // Update statistics
        merged.mean = (hist1.mean + hist2.mean) / 2;
        merged.standardDeviation = Math.max(
            hist1.standardDeviation || 0,
            hist2.standardDeviation || 0
        );
        merged.complexity = Math.max(
            hist1.complexity || 0,
            hist2.complexity || 0
        );
        merged.entropy = (
            (hist1.entropy || 0) + (hist2.entropy || 0)
        ) / 2;
        
        return merged;
    }
    
    /**
     * Merge frequency maps
     */
    mergeFrequencies(freq1, freq2) {
        if (!freq1) return freq2;
        if (!freq2) return freq1;
        
        const merged = { ...freq1 };
        
        for (const [key, value] of Object.entries(freq2)) {
            merged[key] = (merged[key] || 0) + value;
        }
        
        return merged;
    }
    
    /**
     * Merge enum values
     */
    mergeEnumValues(enum1, enum2) {
        if (!enum1) return enum2;
        if (!enum2) return enum1;
        
        const allValues = new Set([...enum1, ...enum2]);
        return Array.from(allValues);
    }
    
    /**
     * Merge anyOf unions
     */
    mergeAnyOf(anyOf1, anyOf2) {
        if (!anyOf1) return anyOf2;
        if (!anyOf2) return anyOf1;
        
        // For now, keep all unique schemas
        // In production, might want to merge similar schemas
        return [...anyOf1, ...anyOf2];
    }
    
    /**
     * Train a model from scratch or update existing
     */
    train(data, existingModel = null) {
        if (!existingModel) {
            // Train from scratch
            return this.inferrer.inferSchema(data);
        } else {
            // Incremental update
            return this.updateModel(existingModel, data);
        }
    }
    
    /**
     * Save model to file
     */
    async saveModel(model, filepath) {
        const { writeFile } = await import('fs/promises');
        const json = JSON.stringify(model, null, 2);
        await writeFile(filepath, json, 'utf8');
    }
    
    /**
     * Load model from file
     */
    async loadModel(filepath) {
        const { readFile } = await import('fs/promises');
        const json = await readFile(filepath, 'utf8');
        return JSON.parse(json);
    }
}

export { IncrementalTrainer };