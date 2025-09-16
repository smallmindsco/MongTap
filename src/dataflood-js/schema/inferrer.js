/**
 * Schema Inferrer Module
 * Port of DataFlood C# SchemaInferrer.cs
 * Infers DataFlood schemas from JSON documents
 */

import { DataFloodModel, DataFloodStringModel, DataFloodHistogram } from '../models/DataFloodModel.js';
import { 
  detectType, 
  detectArrayItemType,
  detectConsistentFormat,
  shouldBeEnum,
  analyzeNumericConstraints,
  analyzeStringConstraints,
  detectPattern,
} from './type-detector.js';
import logger from '../../utils/logger.js';

const log = logger.child('SchemaInferrer');

export class SchemaInferrer {
  constructor(options = {}) {
    this.enumThreshold = options.enumThreshold || 0.5;
    this.detectFormats = options.detectFormats !== false;
    this.detectPatterns = options.detectPatterns !== false;
    this.inferStringModels = options.inferStringModels !== false;
    this.inferHistograms = options.inferHistograms !== false;
    this.detectRelationships = options.detectRelationships || false;
  }
  
  /**
   * Infer schema from array of documents
   * Matches C# InferSchema method
   */
  inferSchema(documents) {
    if (!documents || documents.length === 0) {
      throw new Error('No documents provided for schema inference');
    }
    
    log.debug(`Inferring schema from ${documents.length} documents`);
    
    // Determine root types
    const types = [...new Set(documents.map(detectType))];
    
    let rootSchema;
    if (types.length === 1) {
      rootSchema = this.inferSchemaForType(documents, types[0]);
    } else {
      // Multiple types - create union schema with anyOf
      rootSchema = new DataFloodModel({
        anyOf: types.map(type => {
          const docsOfType = documents.filter(d => detectType(d) === type);
          return this.inferSchemaForType(docsOfType, type);
        }),
      });
    }
    
    // Set schema URL only at root level (matching C#)
    rootSchema.$schema = 'https://smallminds.co/DataFlood/schema#';
    
    return rootSchema;
  }
  
  /**
   * Infer schema for specific type
   * Matches C# InferSchemaForType method
   */
  inferSchemaForType(documents, type, isRoot = false) {
    // Create schema WITHOUT $schema property (only root should have it)
    const schema = new DataFloodModel({ type });
    if (!isRoot) {
      delete schema.$schema; // Remove $schema for non-root schemas
    }
    
    switch (type) {
      case 'object':
        this.inferObjectSchema(schema, documents);
        break;
      case 'array':
        this.inferArraySchema(schema, documents);
        break;
      case 'string':
        this.inferStringSchema(schema, documents);
        break;
      case 'number':
      case 'integer':
        this.inferNumberSchema(schema, documents, type);
        break;
      case 'boolean':
        // Boolean has no additional constraints
        break;
      case 'null':
        schema.type = 'null';
        break;
    }
    
    return schema;
  }
  
  /**
   * Infer object schema
   * Matches C# InferObjectSchema method
   */
  inferObjectSchema(schema, objects) {
    const allProperties = {};
    const requiredProperties = new Set();
    
    // Collect all properties across all objects
    for (const obj of objects) {
      for (const [key, value] of Object.entries(obj)) {
        if (!allProperties[key]) {
          allProperties[key] = [];
        }
        if (value !== null && value !== undefined) {
          allProperties[key].push(value);
        }
      }
    }
    
    // Determine required properties (present in all objects)
    for (const propName of Object.keys(allProperties)) {
      const presentCount = objects.filter(obj => propName in obj).length;
      if (presentCount === objects.length) {
        requiredProperties.add(propName);
      }
    }
    
    // Generate schema for each property
    schema.properties = {};
    for (const [propName, values] of Object.entries(allProperties)) {
      if (values.length > 0) {
        // Infer type from all values
        const propTypes = [...new Set(values.map(detectType))];
        
        if (propTypes.length === 1) {
          schema.properties[propName] = this.inferSchemaForType(values, propTypes[0]);
        } else {
          // Multiple types for this property - use anyOf
          // Don't use DataFloodModel for nested schemas to avoid adding $schema
          schema.properties[propName] = {
            anyOf: propTypes.map(type => {
              const valuesOfType = values.filter(v => detectType(v) === type);
              const subSchema = this.inferSchemaForType(valuesOfType, type);
              // Remove $schema from nested schemas
              delete subSchema.$schema;
              return subSchema;
            })
          };
        }
      }
    }
    
    if (requiredProperties.size > 0) {
      schema.required = Array.from(requiredProperties).sort();
    }
    
    // Detect potential relationships (foreign keys)
    if (this.detectRelationships) {
      const relationships = this.detectPotentialRelationships(schema.properties, objects);
      if (relationships.length > 0) {
        schema.relationships = relationships;
      }
    }
  }
  
  /**
   * Detect potential foreign key relationships based on field naming and patterns
   * This is a heuristic approach for basic relationship detection
   */
  detectPotentialRelationships(properties, objects) {
    const relationships = [];
    const foreignKeyPatterns = [
      /_id$/i,          // user_id, account_id, etc.
      /_ref$/i,         // user_ref, account_ref
      /_key$/i,         // foreign_key
      /^parent_/i,      // parent_id, parent_ref
      /^child_/i,       // child_id
      /^reference_/i,   // reference_number
      /^related_/i,     // related_entity
    ];
    
    for (const [fieldName, fieldSchema] of Object.entries(properties)) {
      // Check if field name matches foreign key patterns
      const isForeignKeyCandidate = foreignKeyPatterns.some(pattern => pattern.test(fieldName));
      
      if (isForeignKeyCandidate) {
        // Analyze the field values
        const fieldValues = objects.map(obj => obj[fieldName]).filter(v => v != null);
        
        // Check if values look like IDs (strings or numbers that are unique or semi-unique)
        if (fieldValues.length > 0) {
          const uniqueValues = new Set(fieldValues);
          const uniqueRatio = uniqueValues.size / fieldValues.length;
          
          // High unique ratio suggests this could be a foreign key
          if (uniqueRatio > 0.5) {
            const relationship = {
              field: fieldName,
              type: 'foreign_key',
              confidence: Math.round(uniqueRatio * 100) / 100,
              valueType: fieldSchema.type || 'unknown',
            };
            
            // Try to infer the referenced entity from field name
            const entityMatch = fieldName.match(/^(.+?)_(?:id|ref|key)$/i);
            if (entityMatch) {
              relationship.referencedEntity = entityMatch[1];
            }
            
            // Check if it's a parent/child relationship
            if (/^parent_/i.test(fieldName)) {
              relationship.relationshipType = 'parent';
            } else if (/^child_/i.test(fieldName)) {
              relationship.relationshipType = 'child';
            } else {
              relationship.relationshipType = 'reference';
            }
            
            relationships.push(relationship);
          }
        }
      }
    }
    
    return relationships;
  }
  
  /**
   * Infer array schema
   * Matches C# InferArraySchema method
   */
  inferArraySchema(schema, arrays) {
    const allItems = [];
    
    // Collect all items from all arrays
    for (const array of arrays) {
      for (const item of array) {
        if (item !== null && item !== undefined) {
          allItems.push(item);
        }
      }
    }
    
    if (allItems.length > 0) {
      // Determine item types
      const itemTypes = [...new Set(allItems.map(detectType))];
      
      if (itemTypes.length === 1) {
        schema.items = this.inferSchemaForType(allItems, itemTypes[0]);
      } else {
        // Multiple item types - use anyOf
        // Don't use DataFloodModel for nested schemas to avoid adding $schema
        schema.items = {
          anyOf: itemTypes.map(type => {
            const itemsOfType = allItems.filter(item => detectType(item) === type);
            const subSchema = this.inferSchemaForType(itemsOfType, type);
            // Remove $schema from nested schemas
            delete subSchema.$schema;
            return subSchema;
          })
        };
      }
    }
    
    // Set min/max items
    const lengths = arrays.map(a => a.length);
    if (lengths.length > 0) {
      const uniqueLengths = [...new Set(lengths)];
      if (uniqueLengths.length === 1) {
        // All arrays same length
        schema.minItems = uniqueLengths[0];
        schema.maxItems = uniqueLengths[0];
      } else {
        schema.minItems = Math.min(...lengths);
        schema.maxItems = Math.max(...lengths);
      }
    }
    
    // Check for unique items
    for (const array of arrays) {
      const uniqueItems = [...new Set(array.map(JSON.stringify))];
      if (uniqueItems.length === array.length) {
        schema.uniqueItems = true;
        break;
      }
    }
  }
  
  /**
   * Infer string schema
   * Matches C# InferStringSchema method
   */
  inferStringSchema(schema, strings) {
    if (strings.length === 0) {
      return;
    }
    
    // Check for enum
    if (shouldBeEnum(strings, this.enumThreshold)) {
      schema.enum = [...new Set(strings)].sort();
      // Don't return early - still need to set min/max length
    }
    
    // String length constraints
    const constraints = analyzeStringConstraints(strings);
    schema.minLength = constraints.minLength;
    schema.maxLength = constraints.maxLength;
    
    // Detect format
    if (this.detectFormats) {
      const format = detectConsistentFormat(strings);
      if (format) {
        schema.format = format;
      }
    }
    
    // Detect pattern
    if (this.detectPatterns && !schema.format) {
      const pattern = detectPattern(strings);
      if (pattern) {
        schema.pattern = pattern;
      }
    }
    
    // Build string model if requested
    if (this.inferStringModels) {
      schema.stringModel = this.buildStringModel(strings);
    }
  }
  
  /**
   * Infer number/integer schema
   * Matches C# InferNumberSchema method
   */
  inferNumberSchema(schema, numbers, type) {
    if (numbers.length === 0) {
      return;
    }
    
    const constraints = analyzeNumericConstraints(numbers);
    schema.minimum = constraints.minimum;
    schema.maximum = constraints.maximum;
    
    if (constraints.multipleOf) {
      schema.multipleOf = constraints.multipleOf;
    }
    
    // Build histogram if requested
    if (this.inferHistograms && numbers.length >= 10) {
      schema.histogram = this.buildHistogram(numbers);
    }
  }
  
  /**
   * Build string model from samples
   * Matches DataFlood C# StringModelGenerator.cs implementation
   */
  buildStringModel(strings, maxSamples = 20) {
    if (!strings || strings.length === 0) {
      return new DataFloodStringModel();
    }
    
    const modelData = {
      minLength: Math.min(...strings.map(s => s.length)),
      maxLength: Math.max(...strings.map(s => s.length)),
      averageLength: Math.round(strings.reduce((sum, s) => sum + s.length, 0) / strings.length * 100) / 100,
      uniqueCharacters: [],
      characterFrequency: {},
      characterProbability: {},
      patterns: {},
      nGrams: {},
      commonPrefixes: {},
      commonSuffixes: {},
      valueFrequency: {},
      sampleValues: [],
      uniqueValues: [],
      totalSamples: strings.length,
      uniqueValueCount: 0,
      entropyScore: 0,
      maxEntropy: 0,
      complexity: 0
    };
    
    // Character frequency analysis
    const charCounts = {};
    let totalChars = 0;
    
    for (const str of strings) {
      for (const char of str) {
        charCounts[char] = (charCounts[char] || 0) + 1;
        totalChars++;
      }
    }
    
    modelData.uniqueCharacters = Object.keys(charCounts).sort();
    modelData.characterFrequency = charCounts;
    
    // Character probability
    for (const [char, count] of Object.entries(charCounts)) {
      modelData.characterProbability[char] = Math.round((count / totalChars) * 10000) / 10000;
    }
    
    // Extract patterns
    const patterns = {};
    for (const str of strings) {
      const pattern = this.generatePattern(str);
      patterns[pattern] = (patterns[pattern] || 0) + 1;
    }
    // Keep top 10 patterns
    modelData.patterns = Object.entries(patterns)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .reduce((obj, [key, val]) => ({ ...obj, [key]: val }), {});
    
    // Analyze prefixes and suffixes
    const prefixes = {};
    const suffixes = {};
    const prefixLength = Math.min(3, modelData.minLength);
    const suffixLength = Math.min(3, modelData.minLength);
    
    if (prefixLength > 0) {
      for (const str of strings) {
        for (let len = 1; len <= Math.min(prefixLength, str.length); len++) {
          const prefix = str.substring(0, len);
          prefixes[prefix] = (prefixes[prefix] || 0) + 1;
        }
      }
    }
    
    if (suffixLength > 0) {
      for (const str of strings) {
        for (let len = 1; len <= Math.min(suffixLength, str.length); len++) {
          const suffix = str.substring(str.length - len);
          suffixes[suffix] = (suffixes[suffix] || 0) + 1;
        }
      }
    }
    
    // Keep common prefixes/suffixes (appearing more than once)
    modelData.commonPrefixes = Object.entries(prefixes)
      .filter(([_, count]) => count > 1)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .reduce((obj, [key, val]) => ({ ...obj, [key]: val }), {});
    
    modelData.commonSuffixes = Object.entries(suffixes)
      .filter(([_, count]) => count > 1)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .reduce((obj, [key, val]) => ({ ...obj, [key]: val }), {});
    
    // Generate n-grams (2-grams and 3-grams)
    const nGrams = {};
    for (const str of strings) {
      for (let n = 2; n <= Math.min(3, str.length); n++) {
        for (let i = 0; i <= str.length - n; i++) {
          const nGram = str.substring(i, i + n);
          nGrams[nGram] = (nGrams[nGram] || 0) + 1;
        }
      }
    }
    
    // Keep top 20 n-grams that appear more than once
    modelData.nGrams = Object.entries(nGrams)
      .filter(([_, count]) => count > 1)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
      .reduce((obj, [key, val]) => ({ ...obj, [key]: val }), {});
    
    // Analyze value frequencies
    const valueFreq = {};
    for (const str of strings) {
      valueFreq[str] = (valueFreq[str] || 0) + 1;
    }
    
    modelData.valueFrequency = valueFreq;
    modelData.uniqueValues = Object.keys(valueFreq);
    modelData.uniqueValueCount = modelData.uniqueValues.length;
    
    // Store sample values (most frequent ones first)
    modelData.sampleValues = Object.entries(valueFreq)
      .sort((a, b) => b[1] - a[1])
      .slice(0, maxSamples)
      .map(([key]) => key);
    
    // Create string model instance and calculate entropy/complexity
    const stringModel = new DataFloodStringModel(modelData);
    stringModel.calculateEntropy();
    stringModel.calculateMaxEntropy();
    stringModel.calculateComplexity();
    
    return stringModel;
  }
  
  /**
   * Generate pattern from string (matching DataFlood C#)
   * d = digit, U = uppercase, L = lowercase, s = space, p = punctuation/special
   */
  generatePattern(input) {
    if (!input) return '';
    
    let pattern = '';
    for (const ch of input) {
      if (/\d/.test(ch)) {
        pattern += 'd';
      } else if (/[A-Z]/.test(ch)) {
        pattern += 'U';
      } else if (/[a-z]/.test(ch)) {
        pattern += 'L';
      } else if (/\s/.test(ch)) {
        pattern += 's';
      } else {
        pattern += 'p'; // punctuation/special
      }
    }
    
    // Compress consecutive patterns (e.g., "ddd" -> "d{3}")
    return pattern.replace(/(.)\1+/g, (match, char) => `${char}{${match.length}}`);
  }
  
  /**
   * Build histogram from numeric samples
   * Matches DataFlood C# HistogramGenerator.cs implementation
   */
  buildHistogram(numbers) {
    if (!numbers || numbers.length === 0) {
      return new DataFloodHistogram();
    }
    
    const sortedNumbers = [...numbers].sort((a, b) => a - b);
    const minValue = sortedNumbers[0];
    const maxValue = sortedNumbers[sortedNumbers.length - 1];
    const range = maxValue - minValue;
    
    const histogramData = {
      bins: [],
      totalCount: numbers.length,
      minValue: minValue,
      maxValue: maxValue,
      complexity: 0,
      standardDeviation: 0,
      entropyScore: 0,
      maxEntropy: 0,
    };
    
    // Calculate standard deviation
    if (numbers.length > 1) {
      const mean = numbers.reduce((sum, n) => sum + n, 0) / numbers.length;
      const sumOfSquares = numbers.reduce((sum, n) => sum + Math.pow(n - mean, 2), 0);
      histogramData.standardDeviation = Math.sqrt(sumOfSquares / (numbers.length - 1));
    }
    
    // Handle case where all values are the same
    if (range === 0) {
      histogramData.bins.push({
        rangeStart: minValue,
        rangeEnd: minValue,
        count: numbers.length,
        freqStart: 0.0,
        freqEnd: 100.0
      });
      const histogram = new DataFloodHistogram(histogramData);
      histogram.calculateEntropy();
      histogram.calculateComplexity();
      return histogram;
    }
    
    // Create bins (default 10 bins)
    const binCount = 10;
    const binWidth = range / binCount;
    let cumulativeFrequency = 0.0;
    
    for (let i = 0; i < binCount; i++) {
      const rangeStart = minValue + (i * binWidth);
      const rangeEnd = i === binCount - 1 ? maxValue : minValue + ((i + 1) * binWidth);
      
      const binNumbers = numbers.filter(n => {
        if (i === binCount - 1) {
          return n >= rangeStart && n <= rangeEnd;
        }
        return n >= rangeStart && n < rangeEnd;
      });
      
      // Only add bins that contain data (frequency > 0)
      if (binNumbers.length > 0) {
        const binFrequencyPercent = Math.round((binNumbers.length / numbers.length) * 100 * 100) / 100;
        const freqStart = cumulativeFrequency;
        const freqEnd = cumulativeFrequency + binFrequencyPercent;
        
        histogramData.bins.push({
          rangeStart: Math.round(rangeStart * 10000) / 10000,
          rangeEnd: Math.round(rangeEnd * 10000) / 10000,
          count: binNumbers.length,
          freqStart: Math.round(freqStart * 100) / 100,
          freqEnd: Math.round(freqEnd * 100) / 100
        });
        
        cumulativeFrequency = freqEnd;
      }
    }
    
    // Create histogram instance and calculate entropy/complexity
    const histogram = new DataFloodHistogram(histogramData);
    histogram.calculateEntropy();
    histogram.calculateComplexity();
    
    return histogram;
  }
  
  /**
   * Merge two schemas
   * Based on DataFlood C# MergeSchemas logic
   */
  mergeSchemas(schema1, schema2) {
    if (!schema1) return schema2;
    if (!schema2) return schema1;
    
    const merged = new DataFloodModel({
      $schema: schema1.$schema || schema2.$schema,
    });
    
    // Merge types
    if (schema1.type === schema2.type) {
      merged.type = schema1.type;
    } else if (schema1.type && schema2.type) {
      // Different types - use anyOf
      merged.anyOf = [
        { type: schema1.type },
        { type: schema2.type }
      ];
    }
    
    // Merge properties for objects
    if (schema1.properties || schema2.properties) {
      merged.properties = {};
      
      // Add all properties from both schemas
      const allProps = new Set([
        ...Object.keys(schema1.properties || {}),
        ...Object.keys(schema2.properties || {})
      ]);
      
      for (const prop of allProps) {
        const prop1 = schema1.properties?.[prop];
        const prop2 = schema2.properties?.[prop];
        
        if (prop1 && prop2) {
          // Property exists in both - merge them
          merged.properties[prop] = this.mergeSchemas(prop1, prop2);
        } else {
          // Property exists in only one schema
          merged.properties[prop] = prop1 || prop2;
        }
      }
    }
    
    // Merge required fields (intersection - only if required in both)
    if (schema1.required && schema2.required) {
      const required = schema1.required.filter(r => schema2.required.includes(r));
      if (required.length > 0) {
        merged.required = required;
      }
    }
    
    // Merge array items
    if (schema1.items || schema2.items) {
      merged.items = this.mergeSchemas(schema1.items, schema2.items);
    }
    
    // Preserve constraints (take most restrictive)
    if (schema1.minimum !== undefined || schema2.minimum !== undefined) {
      merged.minimum = Math.max(schema1.minimum ?? -Infinity, schema2.minimum ?? -Infinity);
    }
    if (schema1.maximum !== undefined || schema2.maximum !== undefined) {
      merged.maximum = Math.min(schema1.maximum ?? Infinity, schema2.maximum ?? Infinity);
    }
    if (schema1.minLength !== undefined || schema2.minLength !== undefined) {
      merged.minLength = Math.max(schema1.minLength ?? 0, schema2.minLength ?? 0);
    }
    if (schema1.maxLength !== undefined || schema2.maxLength !== undefined) {
      merged.maxLength = Math.min(schema1.maxLength ?? Infinity, schema2.maxLength ?? Infinity);
    }
    
    // Merge string models and histograms
    if (schema1.stringModel || schema2.stringModel) {
      // For now, prefer the first schema's model
      merged.stringModel = schema1.stringModel || schema2.stringModel;
    }
    if (schema1.histogram || schema2.histogram) {
      merged.histogram = schema1.histogram || schema2.histogram;
    }
    
    // Merge format and pattern
    merged.format = schema1.format || schema2.format;
    merged.pattern = schema1.pattern || schema2.pattern;
    
    return merged;
  }
}

// Export default instance and class
const defaultInferrer = new SchemaInferrer();
export default defaultInferrer;