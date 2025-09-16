/**
 * DataFlood Model Definitions
 * Port of DataFlood C# model structure for 1:1 compatibility
 * Based on DF.JsonSchemaGenerator.Core/Models/DataFloodModel.cs
 */

// No external dependencies - following design guidelines

/**
 * Main model representing a DataFlood JSON schema configuration
 * Matches C# DataFloodModel class exactly
 */
export class DataFloodModel {
  constructor(data = {}) {
    // JSON Schema version
    this.$schema = data.$schema || 'https://smallminds.co/DataFlood/schema#';
    
    // The type of the root element
    this.type = data.type || null;
    
    // Properties for object types
    this.properties = data.properties ? this._parseProperties(data.properties) : null;
    
    // Required properties for object types
    this.required = data.required || null;
    
    // Schema for array items
    this.items = data.items ? new DataFloodModel(data.items) : null;
    
    // List of possible schemas (union type)
    this.anyOf = data.anyOf ? data.anyOf.map((schema) => new DataFloodModel(schema)) : null;
    
    // Numeric constraints
    this.minimum = data.minimum !== undefined ? data.minimum : null;
    this.maximum = data.maximum !== undefined ? data.maximum : null;
    
    // String constraints
    this.minLength = data.minLength !== undefined ? data.minLength : null;
    this.maxLength = data.maxLength !== undefined ? data.maxLength : null;
    
    // Array constraints
    this.minItems = data.minItems !== undefined ? data.minItems : null;
    this.maxItems = data.maxItems !== undefined ? data.maxItems : null;
    
    // Format hint for string types (e.g., "date-time", "email", "uri")
    this.format = data.format || null;
    
    // Pattern for string validation
    this.pattern = data.pattern || null;
    
    // Multiple of constraint for numeric types
    this.multipleOf = data.multipleOf !== undefined ? data.multipleOf : null;
    
    // Whether array items must be unique
    this.uniqueItems = data.uniqueItems !== undefined ? data.uniqueItems : null;
    
    // Description of this schema element
    this.description = data.description || null;
    
    // Enumeration of allowed values
    this.enum = data.enum || null;
    
    // DataFlood-specific extensions
    this.histogram = data.histogram ? new DataFloodHistogram(data.histogram) : null;
    this.stringModel = data.stringModel ? new DataFloodStringModel(data.stringModel) : null;
    this.tidesConfig = data.tidesConfig ? new TideConfig(data.tidesConfig) : null;
    
    // Default value for optional properties
    this.default = data.default !== undefined ? data.default : null;
  }
  
  _parseProperties(properties) {
    const result = {};
    for (const [key, value] of Object.entries(properties)) {
      result[key] = new DataFloodModel(value);
    }
    return result;
  }
  
  /**
   * Convert model to JSON representation matching C# serialization
   */
  toJSON() {
    const json = {};
    
    // Only include non-null properties to match C# serialization
    if (this.$schema) json.$schema = this.$schema;
    if (this.type) json.type = this.type;
    if (this.properties) {
      json.properties = {};
      for (const [key, value] of Object.entries(this.properties)) {
        json.properties[key] = value.toJSON();
      }
    }
    if (this.required) json.required = this.required;
    if (this.items) json.items = this.items.toJSON();
    if (this.anyOf) json.anyOf = this.anyOf.map((schema) => schema.toJSON());
    if (this.minimum !== null) json.minimum = this.minimum;
    if (this.maximum !== null) json.maximum = this.maximum;
    if (this.minLength !== null) json.minLength = this.minLength;
    if (this.maxLength !== null) json.maxLength = this.maxLength;
    if (this.minItems !== null) json.minItems = this.minItems;
    if (this.maxItems !== null) json.maxItems = this.maxItems;
    if (this.format) json.format = this.format;
    if (this.pattern) json.pattern = this.pattern;
    if (this.multipleOf !== null) json.multipleOf = this.multipleOf;
    if (this.uniqueItems !== null) json.uniqueItems = this.uniqueItems;
    if (this.description) json.description = this.description;
    if (this.enum) json.enum = this.enum;
    if (this.histogram) json.histogram = this.histogram.toJSON();
    if (this.stringModel) json.stringModel = this.stringModel.toJSON();
    if (this.tidesConfig) json.tidesConfig = this.tidesConfig.toJSON();
    if (this.default !== null) json.default = this.default;
    
    return json;
  }
  
  /**
   * Load model from JSON file or object
   */
  static fromJSON(json) {
    if (typeof json === 'string') {
      json = JSON.parse(json);
    }
    return new DataFloodModel(json);
  }
}

/**
 * Histogram model for generating numeric values based on distributions
 * Matches C# DataFloodHistogram class
 */
export class DataFloodHistogram {
  constructor(data = {}) {
    this.bins = data.bins ? data.bins.map((bin) => new DataFloodHistogramBin(bin)) : [];
    this.totalCount = data.totalCount || 0;
    this.minValue = data.minValue !== undefined ? data.minValue : 0;
    this.maxValue = data.maxValue !== undefined ? data.maxValue : 0;
    this.complexity = data.complexity !== undefined ? data.complexity : 0;
    this.standardDeviation = data.standardDeviation !== undefined ? data.standardDeviation : 0;
    this.entropyScore = data.entropyScore !== undefined ? data.entropyScore : 0;
    this.maxEntropy = data.maxEntropy !== undefined ? data.maxEntropy : 0;
  }
  
  toJSON() {
    return {
      bins: this.bins.map((bin) => bin.toJSON()),
      totalCount: this.totalCount,
      minValue: this.minValue,
      maxValue: this.maxValue,
      complexity: this.complexity,
      standardDeviation: this.standardDeviation,
      entropyScore: this.entropyScore,
      maxEntropy: this.maxEntropy,
    };
  }
  
  /**
   * Calculate entropy from the histogram bins
   * Based on DataFlood C# HistogramGenerator.cs
   */
  calculateEntropy() {
    if (this.bins.length === 0 || this.totalCount === 0) {
      this.entropyScore = 0;
      this.maxEntropy = 0;
      return;
    }
    
    let entropy = 0;
    for (const bin of this.bins) {
      if (bin.count > 0) {
        const probability = bin.count / this.totalCount;
        entropy -= probability * Math.log2(probability);
      }
    }
    
    this.entropyScore = Math.round(entropy * 10000) / 10000; // Round to 4 decimal places
    // Maximum entropy for uniform distribution across bins
    this.maxEntropy = Math.round(Math.log2(this.bins.length) * 10000) / 10000;
  }
  
  /**
   * Calculate complexity based on entropy and distribution characteristics
   */
  calculateComplexity() {
    // Complexity is a composite score based on multiple factors
    let complexity = 0;
    
    // Factor 1: Entropy contributes to complexity (40% weight)
    complexity += this.entropyScore * 0.4;
    
    // Factor 2: Number of bins relative to theoretical maximum (20% weight)
    if (this.totalCount > 0) {
      const binRatio = Math.min(this.bins.length / Math.min(this.totalCount, 100), 1.0);
      complexity += binRatio * 0.2;
    }
    
    // Factor 3: Relative standard deviation (20% weight)
    if (this.maxValue !== this.minValue && this.maxValue !== 0) {
      const range = this.maxValue - this.minValue;
      const relativeStdDev = Math.min(this.standardDeviation / range, 1.0);
      complexity += relativeStdDev * 0.2;
    }
    
    // Factor 4: Distribution uniformity (20% weight)
    if (this.bins.length > 0 && this.totalCount > 0) {
      const expectedCount = this.totalCount / this.bins.length;
      let variance = 0;
      for (const bin of this.bins) {
        const diff = (bin.count - expectedCount) / expectedCount;
        variance += diff * diff;
      }
      variance /= this.bins.length;
      const uniformity = Math.max(0, 1 - Math.min(variance, 1));
      complexity += uniformity * 0.2;
    }
    
    this.complexity = Math.round(complexity * 10000) / 10000;
  }
}

/**
 * Individual bin in a histogram
 * Matches C# DataFloodHistogramBin class
 */
export class DataFloodHistogramBin {
  constructor(data = {}) {
    this.rangeStart = data.rangeStart !== undefined ? data.rangeStart : 0;
    this.rangeEnd = data.rangeEnd !== undefined ? data.rangeEnd : 0;
    this.count = data.count || 0;
    this.freqStart = data.freqStart !== undefined ? data.freqStart : 0;
    this.freqEnd = data.freqEnd !== undefined ? data.freqEnd : 0;
  }
  
  toJSON() {
    return {
      rangeStart: this.rangeStart,
      rangeEnd: this.rangeEnd,
      count: this.count,
      freqStart: this.freqStart,
      freqEnd: this.freqEnd,
    };
  }
}

/**
 * String model for generating text values based on patterns and statistics
 * Matches C# DataFloodStringModel class
 */
export class DataFloodStringModel {
  constructor(data = {}) {
    this.minLength = data.minLength || 0;
    this.maxLength = data.maxLength || 0;
    this.averageLength = data.averageLength || 0;
    this.uniqueCharacters = data.uniqueCharacters || [];
    this.characterFrequency = data.characterFrequency || {};
    this.characterProbability = data.characterProbability || {};
    this.patterns = data.patterns || {};
    this.nGrams = data.nGrams || data.NGrams || {}; // Support both cases
    this.commonPrefixes = data.commonPrefixes || {};
    this.commonSuffixes = data.commonSuffixes || {};
    this.prefixes = data.prefixes || {};
    this.suffixes = data.suffixes || {};
    this.valueFrequency = data.valueFrequency || {};
    this.sampleValues = data.sampleValues || [];
    this.uniqueValues = data.uniqueValues || [];
    this.totalSamples = data.totalSamples || 0;
    this.uniqueValueCount = data.uniqueValueCount || 0;
    this.entropyScore = data.entropyScore !== undefined ? data.entropyScore : 0;
    this.maxEntropy = data.maxEntropy !== undefined ? data.maxEntropy : 0;
    this.complexity = data.complexity !== undefined ? data.complexity : 0;
    this.entropyOverride = data.entropyOverride !== undefined ? data.entropyOverride : null;
  }
  
  toJSON() {
    const json = {
      minLength: this.minLength,
      maxLength: this.maxLength,
      averageLength: this.averageLength,
      uniqueCharacters: this.uniqueCharacters,
    };
    
    // Only include non-empty objects/arrays
    if (Object.keys(this.characterFrequency).length > 0) {
      json.characterFrequency = this.characterFrequency;
    }
    if (Object.keys(this.characterProbability).length > 0) {
      json.characterProbability = this.characterProbability;
    }
    if (Object.keys(this.patterns).length > 0) {
      json.patterns = this.patterns;
    }
    if (Object.keys(this.nGrams).length > 0) {
      json.nGrams = this.nGrams;
    }
    if (Object.keys(this.commonPrefixes).length > 0) {
      json.commonPrefixes = this.commonPrefixes;
    }
    if (Object.keys(this.commonSuffixes).length > 0) {
      json.commonSuffixes = this.commonSuffixes;
    }
    if (Object.keys(this.prefixes).length > 0) {
      json.prefixes = this.prefixes;
    }
    if (Object.keys(this.suffixes).length > 0) {
      json.suffixes = this.suffixes;
    }
    if (Object.keys(this.valueFrequency).length > 0) {
      json.valueFrequency = this.valueFrequency;
    }
    if (this.sampleValues.length > 0) {
      json.sampleValues = this.sampleValues;
    }
    if (this.uniqueValues.length > 0) {
      json.uniqueValues = this.uniqueValues;
    }
    
    json.totalSamples = this.totalSamples;
    json.uniqueValueCount = this.uniqueValueCount;
    json.entropyScore = this.entropyScore;
    json.maxEntropy = this.maxEntropy;
    json.complexity = this.complexity;
    
    if (this.entropyOverride !== null) {
      json.entropyOverride = this.entropyOverride;
    }
    
    return json;
  }
  
  /**
   * Calculate entropy for the string model based on value frequency
   * Based on DataFlood C# StringModelGenerator.cs
   */
  calculateEntropy() {
    if (this.valueFrequency && Object.keys(this.valueFrequency).length > 0 && this.totalSamples > 0) {
      let entropy = 0;
      
      for (const freq of Object.values(this.valueFrequency)) {
        const probability = freq / this.totalSamples;
        if (probability > 0) {
          entropy -= probability * Math.log2(probability);
        }
      }
      
      this.entropyScore = Math.round(entropy * 10000) / 10000;
    } else {
      this.entropyScore = 0;
    }
  }
  
  /**
   * Calculate maximum entropy seen across all values
   * Based on DataFlood C# StringModelGenerator.cs
   */
  calculateMaxEntropy() {
    // Track the maximum entropy seen across all values
    let maxEntropy = this.entropyScore;
    
    // Calculate individual string entropies to find the maximum
    if (this.sampleValues && this.sampleValues.length > 0) {
      for (const value of this.sampleValues) {
        const charCounts = {};
        for (const char of value) {
          charCounts[char] = (charCounts[char] || 0) + 1;
        }
        
        let entropy = 0;
        const length = value.length;
        
        for (const count of Object.values(charCounts)) {
          const probability = count / length;
          if (probability > 0) {
            entropy -= probability * Math.log2(probability);
          }
        }
        
        maxEntropy = Math.max(maxEntropy, entropy);
      }
    }
    
    // Theoretical maximum based on character set size
    if (this.uniqueCharacters && this.uniqueCharacters.length > 0) {
      const theoreticalMax = Math.log2(this.uniqueCharacters.length);
      maxEntropy = Math.max(maxEntropy, theoreticalMax);
    }
    
    this.maxEntropy = Math.round(maxEntropy * 10000) / 10000;
  }
  
  /**
   * Calculate complexity for the string model
   * Based on DataFlood C# StringModelGenerator.cs
   */
  calculateComplexity() {
    // Complexity is a composite score based on multiple factors
    let complexity = 0;
    
    // Factor 1: Entropy contributes to complexity (normalized)
    // Normalize entropy by dividing by theoretical max (log2 of unique value count)
    if (this.uniqueValueCount > 1) {
      const normalizedEntropy = this.entropyScore / Math.log2(this.uniqueValueCount);
      complexity += Math.min(normalizedEntropy, 1.0) * 0.3;
    } else {
      complexity += 0; // No entropy for single value
    }
    
    // Factor 2: Length variability
    if (this.averageLength > 0) {
      const lengthVariability = (this.maxLength - this.minLength) / this.averageLength;
      complexity += Math.min(lengthVariability, 1.0) * 0.2;
    }
    
    // Factor 3: Character diversity
    if (this.totalSamples > 0 && this.uniqueCharacters) {
      const charDiversity = this.uniqueCharacters.length / Math.min(this.averageLength * 2, 100);
      complexity += Math.min(charDiversity, 1.0) * 0.2;
    }
    
    // Factor 4: Pattern diversity
    if (this.patterns && Object.keys(this.patterns).length > 0) {
      const patternDiversity = Math.min(Object.keys(this.patterns).length / 10, 1.0);
      complexity += patternDiversity * 0.15;
    }
    
    // Factor 5: Value uniqueness
    if (this.totalSamples > 0) {
      const uniquenessRatio = this.uniqueValueCount / this.totalSamples;
      complexity += uniquenessRatio * 0.15;
    }
    
    // Ensure complexity stays within 0-1 range
    this.complexity = Math.round(Math.min(complexity, 1.0) * 10000) / 10000;
  }
}

/**
 * Tides configuration for time-based generation and relationships
 * Placeholder for now - will implement when we study TideService.cs
 */
export class TideConfig {
  constructor(data = {}) {
    this.data = data;
  }
  
  toJSON() {
    return this.data;
  }
}

// Export all classes
export default DataFloodModel;