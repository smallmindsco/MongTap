/**
 * Document Validator - Port of DataFlood's DocumentValidator.cs
 * Validates documents against learned model norms to detect outliers
 */

class DocumentValidator {
    constructor(schema, options = {}) {
        if (!schema) {
            throw new Error('Schema is required');
        }
        
        this.schema = schema;
        this.options = {
            maxDeviationThreshold: options.maxDeviationThreshold || 2.0,
            complexityMultiplier: options.complexityMultiplier || 1.5,
            checkUnknownCharacters: options.checkUnknownCharacters !== false,
            checkDistribution: options.checkDistribution !== false,
            maxZScore: options.maxZScore || 3.0,
            ...options
        };
    }
    
    /**
     * Validates a document against the model's learned norms
     * @param {Object} document - The document to validate
     * @returns {ValidationResult}
     */
    validateDocument(document) {
        const result = {
            isValid: true,
            outliers: [],
            complexityScore: 0
        };
        
        this.validateObject(this.schema, document, '', result);
        
        // Calculate overall complexity score
        if (result.outliers.length > 0) {
            const totalDeviation = result.outliers.reduce((sum, o) => sum + o.deviationScore, 0);
            result.complexityScore = totalDeviation / result.outliers.length;
            result.isValid = result.complexityScore <= this.options.maxDeviationThreshold;
        }
        
        return result;
    }
    
    /**
     * Validate an object against schema
     */
    validateObject(schema, node, path, result) {
        if (node == null) return;
        
        if (schema.properties && typeof node === 'object' && !Array.isArray(node)) {
            for (const [key, propSchema] of Object.entries(schema.properties)) {
                const propPath = path ? `${path}.${key}` : key;
                
                if (key in node) {
                    this.validateProperty(propSchema, node[key], propPath, result);
                }
            }
        } else if (schema.type === 'array' && Array.isArray(node)) {
            for (let i = 0; i < node.length; i++) {
                this.validateProperty(schema.items || schema, node[i], `${path}[${i}]`, result);
            }
        }
    }
    
    /**
     * Validate a property against schema
     */
    validateProperty(schema, value, path, result) {
        if (value == null) return;
        
        const type = schema.type?.toLowerCase();
        
        switch (type) {
            case 'string':
                this.validateString(schema, value, path, result);
                break;
            case 'number':
            case 'integer':
                this.validateNumber(schema, value, path, result);
                break;
            case 'object':
                this.validateObject(schema, value, path, result);
                break;
            case 'array':
                if (Array.isArray(value)) {
                    for (let i = 0; i < value.length; i++) {
                        this.validateProperty(schema.items || schema, value[i], `${path}[${i}]`, result);
                    }
                }
                break;
        }
    }
    
    /**
     * Validate string value against string model
     */
    validateString(schema, value, path, result) {
        if (!schema.stringModel) return;
        
        const stringValue = String(value);
        const model = schema.stringModel;
        
        // If value is from the training set, it's inherently valid
        const isFromTrainingSet = model.valueFrequency && 
                                  model.valueFrequency[stringValue] !== undefined;
        if (isFromTrainingSet) {
            return; // Value is from training data, consider it valid
        }
        
        // Check entropy override for sensitivity adjustment
        const effectiveEntropy = model.entropyOverride ?? model.entropyScore;
        
        // Calculate complexity of the actual value
        const valueComplexity = this.calculateStringComplexity(stringValue, model);
        
        // Check if value is an outlier
        if (valueComplexity > model.complexity * this.options.complexityMultiplier) {
            const deviation = (valueComplexity - model.complexity) / model.complexity;
            
            result.outliers.push({
                path: path,
                fieldType: 'string',
                expectedComplexity: model.complexity,
                actualComplexity: valueComplexity,
                deviationScore: deviation,
                value: stringValue,
                reason: this.determineOutlierReason(stringValue, model)
            });
        }
        
        // Check length bounds
        if (stringValue.length < model.minLength || stringValue.length > model.maxLength) {
            result.outliers.push({
                path: path,
                fieldType: 'string',
                expectedComplexity: model.complexity,
                actualComplexity: valueComplexity,
                deviationScore: 1.0,
                value: stringValue,
                reason: `Length ${stringValue.length} outside bounds [${model.minLength}, ${model.maxLength}]`
            });
        }
        
        // Check for unknown characters
        if (this.options.checkUnknownCharacters && model.uniqueCharacters) {
            const unknownChars = [];
            for (const char of stringValue) {
                if (!model.uniqueCharacters.includes(char) && !unknownChars.includes(char)) {
                    unknownChars.push(char);
                }
            }
            
            if (unknownChars.length > 0) {
                result.outliers.push({
                    path: path,
                    fieldType: 'string',
                    expectedComplexity: model.complexity,
                    actualComplexity: valueComplexity,
                    deviationScore: 0.5,
                    value: stringValue,
                    reason: `Contains unknown characters: ${unknownChars.join(', ')}`
                });
            }
        }
    }
    
    /**
     * Validate number value against histogram
     */
    validateNumber(schema, value, path, result) {
        if (!schema.histogram) return;
        
        // Handle both integer and number types
        let numValue;
        try {
            numValue = Number(value);
            if (isNaN(numValue)) return;
        } catch {
            return; // Not a number, skip validation
        }
        
        const histogram = schema.histogram;
        
        // Check if value is within learned bounds
        if (numValue < histogram.minValue || numValue > histogram.maxValue) {
            const range = histogram.maxValue - histogram.minValue;
            const deviation = Math.max(
                Math.abs(numValue - histogram.minValue) / range,
                Math.abs(numValue - histogram.maxValue) / range
            );
            
            result.outliers.push({
                path: path,
                fieldType: 'number',
                expectedComplexity: histogram.complexity,
                actualComplexity: deviation * histogram.complexity,
                deviationScore: deviation,
                value: String(numValue),
                reason: `Value ${numValue} outside range [${histogram.minValue}, ${histogram.maxValue}]`
            });
        }
        
        // Check if value falls in expected distribution
        if (this.options.checkDistribution && histogram.standardDeviation > 0) {
            const mean = (histogram.minValue + histogram.maxValue) / 2;
            const zScore = Math.abs((numValue - mean) / histogram.standardDeviation);
            
            if (zScore > this.options.maxZScore) {
                result.outliers.push({
                    path: path,
                    fieldType: 'number',
                    expectedComplexity: histogram.complexity,
                    actualComplexity: zScore,
                    deviationScore: zScore / this.options.maxZScore,
                    value: String(numValue),
                    reason: `Z-score ${zScore.toFixed(2)} exceeds threshold`
                });
            }
        }
    }
    
    /**
     * Calculate string complexity
     */
    calculateStringComplexity(value, model) {
        if (!value) return 0;
        
        // Calculate Shannon entropy of the value
        const charFreq = {};
        for (const char of value) {
            charFreq[char] = (charFreq[char] || 0) + 1;
        }
        
        let entropy = 0;
        const len = value.length;
        for (const count of Object.values(charFreq)) {
            const prob = count / len;
            entropy -= prob * Math.log2(prob);
        }
        
        // Factor in length deviation
        const avgLength = (model.minLength + model.maxLength) / 2;
        const lengthDeviation = Math.abs(value.length - avgLength) / avgLength;
        
        // Combine entropy and length deviation for complexity score
        return entropy * (1 + lengthDeviation);
    }
    
    /**
     * Determine reason for outlier
     */
    determineOutlierReason(value, model) {
        const reasons = [];
        
        // Check if it's too random
        const charFreq = {};
        for (const char of value) {
            charFreq[char] = (charFreq[char] || 0) + 1;
        }
        
        const uniqueChars = Object.keys(charFreq).length;
        const randomness = uniqueChars / value.length;
        
        if (randomness > 0.8) {
            reasons.push('High randomness');
        }
        
        // Check for unusual patterns
        if (model.patterns && model.patterns.length > 0) {
            const matchesPattern = model.patterns.some(p => {
                try {
                    const regex = new RegExp(p.pattern);
                    return regex.test(value);
                } catch {
                    return false;
                }
            });
            
            if (!matchesPattern) {
                reasons.push('Does not match learned patterns');
            }
        }
        
        // Check character distribution
        if (model.characterFrequency) {
            let unexpectedChars = 0;
            for (const char of value) {
                if (!model.characterFrequency[char]) {
                    unexpectedChars++;
                }
            }
            
            if (unexpectedChars / value.length > 0.3) {
                reasons.push('Unusual character distribution');
            }
        }
        
        return reasons.length > 0 ? reasons.join('; ') : 'Value deviates from learned norms';
    }
}

/**
 * Validation options
 */
class ValidationOptions {
    constructor(options = {}) {
        this.maxDeviationThreshold = options.maxDeviationThreshold || 2.0;
        this.complexityMultiplier = options.complexityMultiplier || 1.5;
        this.checkUnknownCharacters = options.checkUnknownCharacters !== false;
        this.checkDistribution = options.checkDistribution !== false;
        this.maxZScore = options.maxZScore || 3.0;
    }
}

/**
 * Validation result
 */
class ValidationResult {
    constructor() {
        this.isValid = true;
        this.outliers = [];
        this.complexityScore = 0;
    }
}

/**
 * Outlier field information
 */
class OutlierField {
    constructor(data = {}) {
        this.path = data.path || '';
        this.fieldType = data.fieldType || '';
        this.expectedComplexity = data.expectedComplexity || 0;
        this.actualComplexity = data.actualComplexity || 0;
        this.deviationScore = data.deviationScore || 0;
        this.value = data.value || '';
        this.reason = data.reason || '';
    }
}

export { DocumentValidator, ValidationOptions, ValidationResult, OutlierField };