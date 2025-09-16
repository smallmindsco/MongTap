/**
 * Document Generator Module
 * Port of DataFlood C# DocumentGenerator.cs
 * Generates JSON documents from DataFlood schemas
 */

import { DataFloodModel } from '../models/DataFloodModel.js';
import logger from '../../utils/logger.js';

const log = logger.child('DocumentGenerator');

export class DocumentGenerator {
  constructor(seed = null, entropyOverride = null) {
    this.random = seed ? this.seededRandom(seed) : Math.random;
    this.entropyOverride = entropyOverride;
    this.sequentialCounters = {};
    this.modelDocuments = {};
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
  
  /**
   * Generate multiple documents from a schema
   */
  generateDocuments(schema, count = 1, parentDocument = null) {
    const documents = [];
    
    try {
      for (let i = 0; i < count; i++) {
        const doc = this.generateDocument(schema, parentDocument);
        if (doc) {
          documents.push(doc);
        }
      }
    } catch (error) {
      // Validate schema if generation fails
      const validationErrors = this.validateSchema(schema);
      if (validationErrors.length > 0) {
        const errorMessage = `Schema validation failed:\n${validationErrors.join('\n')}`;
        throw new Error(errorMessage);
      }
      throw error;
    }
    
    return documents;
  }
  
  /**
   * Generate a single document from a schema
   */
  generateDocument(schema, parentDocument = null) {
    if (schema.type === 'object' && schema.properties) {
      return this.generateObject(schema, parentDocument);
    }
    
    // If not an object schema, generate the value directly
    const value = this.generateValue(schema);
    return value;
  }
  
  /**
   * Generate an object from schema
   */
  generateObject(schema, parentDocument = null) {
    const obj = {};
    
    if (!schema.properties) {
      return obj;
    }
    
    // Generate each property
    for (const [key, propSchema] of Object.entries(schema.properties)) {
      // Check if property should be generated (required or random chance)
      if (this.shouldGenerateProperty(key, schema.required)) {
        const value = this.generateValue(propSchema);
        // Include all values except undefined (null is a valid value)
        if (value !== undefined) {
          obj[key] = value;
        }
      }
    }
    
    return obj;
  }
  
  /**
   * Determine if a property should be generated
   */
  shouldGenerateProperty(propertyName, requiredFields = []) {
    // Always generate required fields
    if (requiredFields && requiredFields.includes(propertyName)) {
      return true;
    }
    
    // For optional fields, generate with 80% probability
    return this.random() < 0.8;
  }
  
  /**
   * Generate a value based on schema type
   */
  generateValue(schema) {
    // Handle enum values first
    if (schema.enum && schema.enum.length > 0) {
      return this.generateFromEnum(schema.enum);
    }
    
    // Handle anyOf unions
    if (schema.anyOf && schema.anyOf.length > 0) {
      return this.generateFromUnion(schema);
    }
    
    // Generate based on type
    switch (schema.type) {
      case 'string':
        return this.generateString(schema);
      case 'integer':
        return this.generateInteger(schema);
      case 'number':
        return this.generateNumber(schema);
      case 'boolean':
        return this.random() > 0.5;
      case 'array':
        return this.generateArray(schema);
      case 'object':
        return this.generateObject(schema);
      case 'null':
        return null;
      default:
        return null;
    }
  }
  
  /**
   * Generate a string value
   */
  generateString(schema) {
    // Check for format-based generation
    if (schema.format) {
      return this.generateFormattedString(schema.format, schema);
    }
    
    // Check for pattern-based generation
    if (schema.pattern) {
      return this.generateStringFromPattern(schema.pattern);
    }
    
    // Use string model if available
    if (schema.stringModel) {
      return this.generateStringFromModel(schema.stringModel);
    }
    
    // Fallback to random string
    const minLength = schema.minLength || 5;
    const maxLength = schema.maxLength || 20;
    const length = Math.floor(this.random() * (maxLength - minLength + 1)) + minLength;
    
    return this.generateRandomString(length);
  }
  
  /**
   * Generate string from DataFloodStringModel
   */
  generateStringFromModel(model) {
    const effectiveEntropy = this.entropyOverride !== null ? this.entropyOverride : model.entropyScore;
    
    // Low entropy - sample from existing values
    if (effectiveEntropy < 2.0 && model.valueFrequency && Object.keys(model.valueFrequency).length > 0) {
      return this.sampleFromFrequency(model.valueFrequency);
    }
    
    // Medium entropy - use patterns
    if (effectiveEntropy < 4.0 && model.patterns && Object.keys(model.patterns).length > 0) {
      const pattern = this.sampleFromFrequency(model.patterns);
      return this.generateFromPattern(pattern, model);
    }
    
    // High entropy - use character probabilities
    return this.generateFromCharacterProbabilities(model);
  }
  
  /**
   * Sample from frequency distribution
   */
  sampleFromFrequency(frequency) {
    const entries = Object.entries(frequency);
    if (entries.length === 0) return '';
    
    const totalWeight = entries.reduce((sum, [_, weight]) => sum + weight, 0);
    const randomValue = this.random() * totalWeight;
    
    let currentWeight = 0;
    for (const [value, weight] of entries) {
      currentWeight += weight;
      if (randomValue < currentWeight) {
        return value;
      }
    }
    
    return entries[0][0];
  }
  
  /**
   * Generate string from pattern (like U{3}pd{3})
   */
  generateFromPattern(pattern, model) {
    let result = '';
    let i = 0;
    
    while (i < pattern.length) {
      const ch = pattern[i];
      
      // Check for repetition like {3}
      if (i + 1 < pattern.length && pattern[i + 1] === '{') {
        const endBrace = pattern.indexOf('}', i + 2);
        if (endBrace > 0) {
          const countStr = pattern.substring(i + 2, endBrace);
          const count = parseInt(countStr);
          if (!isNaN(count)) {
            for (let j = 0; j < count; j++) {
              result += this.generateCharacterByType(ch, model);
            }
            i = endBrace + 1;
            continue;
          }
        }
      }
      
      result += this.generateCharacterByType(ch, model);
      i++;
    }
    
    return result;
  }
  
  /**
   * Generate character by pattern type
   * Based on DataFlood C# GenerateCharacterByType
   */
  generateCharacterByType(type, model) {
    const chars = model?.uniqueCharacters || [];
    
    switch (type) {
      case 'U': // Uppercase letter
        const upperChars = chars.filter(c => /[A-Z]/.test(c));
        return upperChars.length > 0 ? 
          upperChars[Math.floor(this.random() * upperChars.length)] : 
          String.fromCharCode(65 + Math.floor(this.random() * 26));
          
      case 'L': // Lowercase letter (capital L in pattern)
        const lowerChars = chars.filter(c => /[a-z]/.test(c));
        return lowerChars.length > 0 ?
          lowerChars[Math.floor(this.random() * lowerChars.length)] :
          String.fromCharCode(97 + Math.floor(this.random() * 26));
          
      case 'd': // Digit
        const digitChars = chars.filter(c => /[0-9]/.test(c));
        return digitChars.length > 0 ?
          digitChars[Math.floor(this.random() * digitChars.length)] :
          String.fromCharCode(48 + Math.floor(this.random() * 10));
          
      case 's': // Space character
        return ' ';
        
      case 'p': // Punctuation/special character
        const punctChars = chars.filter(c => /[^A-Za-z0-9\s]/.test(c));
        return punctChars.length > 0 ?
          punctChars[Math.floor(this.random() * punctChars.length)] :
          '.';
          
      default:
        // All other characters are treated as literals
        // This includes lowercase letters in patterns
        return type;
    }
  }
  
  /**
   * Generate string from character probabilities
   */
  generateFromCharacterProbabilities(model) {
    const minLength = model.minLength || 5;
    const maxLength = model.maxLength || 20;
    const length = Math.floor(this.random() * (maxLength - minLength + 1)) + minLength;
    
    let result = '';
    
    // Use n-grams if available
    if (model.nGrams && Object.keys(model.nGrams).length > 0) {
      // Start with a common prefix if available
      if (model.commonPrefixes && Object.keys(model.commonPrefixes).length > 0) {
        const prefix = this.sampleFromFrequency(model.commonPrefixes);
        result = prefix;
      }
      
      // Build using n-grams (with safety limit)
      let iterations = 0;
      const maxIterations = length * 10; // Safety limit to prevent infinite loops
      
      while (result.length < length && iterations < maxIterations) {
        iterations++;
        const lastTwo = result.slice(-2);
        const candidates = Object.keys(model.nGrams).filter(gram => gram.startsWith(lastTwo));
        
        if (candidates.length > 0) {
          const nextGram = candidates[Math.floor(this.random() * candidates.length)];
          const addition = nextGram.slice(lastTwo.length);
          if (addition.length > 0) {
            result += addition;
          } else {
            // If no characters would be added, use fallback
            if (model.uniqueCharacters && model.uniqueCharacters.length > 0) {
              result += model.uniqueCharacters[Math.floor(this.random() * model.uniqueCharacters.length)];
            } else {
              result += String.fromCharCode(97 + Math.floor(this.random() * 26));
            }
          }
        } else {
          // Fallback to random character
          if (model.uniqueCharacters && model.uniqueCharacters.length > 0) {
            result += model.uniqueCharacters[Math.floor(this.random() * model.uniqueCharacters.length)];
          } else {
            result += String.fromCharCode(97 + Math.floor(this.random() * 26));
          }
        }
      }
    } else if (model.characterProbability && Object.keys(model.characterProbability).length > 0) {
      // Use character probabilities
      for (let i = 0; i < length; i++) {
        result += this.sampleFromFrequency(model.characterProbability);
      }
    } else {
      // Fallback to random characters from unique set
      const chars = model.uniqueCharacters || ['a', 'b', 'c', 'd', 'e'];
      for (let i = 0; i < length; i++) {
        result += chars[Math.floor(this.random() * chars.length)];
      }
    }
    
    // Add common suffix if appropriate
    if (model.commonSuffixes && Object.keys(model.commonSuffixes).length > 0 && this.random() < 0.3) {
      const suffix = this.sampleFromFrequency(model.commonSuffixes);
      result = result.slice(0, -suffix.length) + suffix;
    }
    
    return result.slice(0, length);
  }
  
  /**
   * Generate string from regex pattern
   */
  generateStringFromPattern(pattern) {
    let result = '';
    let i = 0;
    
    while (i < pattern.length) {
      if (i < pattern.length - 1 && pattern[i] === '\\') {
        // Check for \d{n} pattern first
        if (pattern[i + 1] === 'd' && i + 2 < pattern.length && pattern[i + 2] === '{') {
          const closeBrace = pattern.indexOf('}', i + 3);
          if (closeBrace > i + 2) {
            const repStr = pattern.substring(i + 3, closeBrace);
            const rep = parseInt(repStr);
            if (!isNaN(rep)) {
              for (let j = 0; j < rep; j++) {
                result += String.fromCharCode(48 + Math.floor(this.random() * 10));
              }
              i = closeBrace + 1;
              continue;
            }
          }
        }
        
        // Handle other escaped characters
        switch (pattern[i + 1]) {
          case 'd': // Digit
            result += String.fromCharCode(48 + Math.floor(this.random() * 10));
            i += 2;
            break;
          case 'w': // Word character
            const wordChars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_';
            result += wordChars[Math.floor(this.random() * wordChars.length)];
            i += 2;
            break;
          case 's': // Whitespace
            result += ' ';
            i += 2;
            break;
          default:
            // Literal escaped character
            result += pattern[i + 1];
            i += 2;
            break;
        }
      } else if (pattern[i] === '[') {
        // Handle character class [A-Z], [0-9], etc.
        const closeIndex = pattern.indexOf(']', i);
        if (closeIndex > i) {
          const charClass = pattern.substring(i + 1, closeIndex);
          let repetition = 1;
          
          // Check for repetition {n} after ]
          if (closeIndex + 1 < pattern.length && pattern[closeIndex + 1] === '{') {
            const closeBrace = pattern.indexOf('}', closeIndex + 2);
            if (closeBrace > closeIndex + 1) {
              const repStr = pattern.substring(closeIndex + 2, closeBrace);
              const rep = parseInt(repStr);
              if (!isNaN(rep)) {
                repetition = rep;
                i = closeBrace + 1;
              } else {
                i = closeIndex + 1;
              }
            } else {
              i = closeIndex + 1;
            }
          } else {
            i = closeIndex + 1;
          }
          
          // Generate characters from class
          for (let j = 0; j < repetition; j++) {
            result += this.generateFromCharacterClass(charClass);
          }
        } else {
          result += pattern[i];
          i++;
        }
      } else {
        // Literal character
        result += pattern[i];
        i++;
      }
    }
    
    return result;
  }
  
  /**
   * Generate character from character class like A-Z
   */
  generateFromCharacterClass(charClass) {
    // Handle ranges like A-Z, a-z, 0-9
    if (charClass.length >= 3 && charClass[1] === '-') {
      const start = charClass.charCodeAt(0);
      const end = charClass.charCodeAt(2);
      return String.fromCharCode(start + Math.floor(this.random() * (end - start + 1)));
    }
    
    // Handle multiple characters
    const chars = [];
    let i = 0;
    while (i < charClass.length) {
      if (i + 2 < charClass.length && charClass[i + 1] === '-') {
        // Range
        const start = charClass.charCodeAt(i);
        const end = charClass.charCodeAt(i + 2);
        for (let c = start; c <= end; c++) {
          chars.push(String.fromCharCode(c));
        }
        i += 3;
      } else {
        // Single character
        chars.push(charClass[i]);
        i++;
      }
    }
    
    return chars.length > 0 ? chars[Math.floor(this.random() * chars.length)] : 'X';
  }
  
  /**
   * Generate an integer value
   */
  generateInteger(schema) {
    const min = schema.minimum !== undefined ? Math.floor(schema.minimum) : 0;
    const max = schema.maximum !== undefined ? Math.floor(schema.maximum) : 100;
    
    // Use histogram if available
    if (schema.histogram && schema.histogram.bins && schema.histogram.bins.length > 0) {
      return Math.floor(this.generateFromHistogram(schema.histogram));
    }
    
    // Handle multipleOf constraint
    if (schema.multipleOf) {
      // Find the first valid multiple >= min
      const firstMultiple = Math.ceil(min / schema.multipleOf) * schema.multipleOf;
      
      // Find the last valid multiple <= max
      const lastMultiple = Math.floor(max / schema.multipleOf) * schema.multipleOf;
      
      if (firstMultiple > max || lastMultiple < min) {
        // No valid multiples in range, return min
        return min;
      }
      
      // Calculate how many valid multiples exist
      const numSteps = Math.floor((lastMultiple - firstMultiple) / schema.multipleOf) + 1;
      
      // Select a random step
      const step = Math.floor(this.random() * numSteps);
      
      return firstMultiple + (step * schema.multipleOf);
    }
    
    return Math.floor(this.random() * (max - min + 1)) + min;
  }
  
  /**
   * Generate a number value
   */
  generateNumber(schema) {
    const min = schema.minimum !== undefined ? schema.minimum : 0.0;
    const max = schema.maximum !== undefined ? schema.maximum : 100.0;
    
    // Use histogram if available
    if (schema.histogram && schema.histogram.bins && schema.histogram.bins.length > 0) {
      return this.generateFromHistogram(schema.histogram);
    }
    
    const value = min + (this.random() * (max - min));
    
    // Handle multipleOf constraint
    if (schema.multipleOf) {
      const rounded = Math.round(value / schema.multipleOf) * schema.multipleOf;
      // Fix floating point precision issues
      const precision = schema.multipleOf.toString().split('.')[1]?.length || 0;
      return Math.round(rounded * Math.pow(10, precision)) / Math.pow(10, precision);
    }
    
    return Math.round(value * 100) / 100; // Round to 2 decimal places
  }
  
  /**
   * Generate value from histogram distribution
   */
  generateFromHistogram(histogram) {
    const randomPercentage = this.random() * 100.0;
    
    // Find the bin that contains this percentage
    for (const bin of histogram.bins) {
      if (randomPercentage >= bin.freqStart && randomPercentage < bin.freqEnd) {
        // Generate random value within this bin's range
        const binMin = bin.rangeStart;
        const binMax = bin.rangeEnd;
        const value = binMin + (this.random() * (binMax - binMin));
        return Math.round(value * 100) / 100;
      }
    }
    
    // Fallback - use last bin
    const lastBin = histogram.bins[histogram.bins.length - 1];
    const fallbackMin = lastBin.rangeStart;
    const fallbackMax = lastBin.rangeEnd;
    const fallbackValue = fallbackMin + (this.random() * (fallbackMax - fallbackMin));
    return Math.round(fallbackValue * 100) / 100;
  }
  
  /**
   * Generate an array
   * Matches DataFlood's GenerateArray implementation
   */
  generateArray(schema) {
    if (!schema.items) {
      return [];
    }
    
    const minItems = schema.minItems || 1;
    const maxItems = schema.maxItems || 5;
    const itemCount = Math.floor(this.random() * (maxItems - minItems + 1)) + minItems;
    
    const array = [];
    for (let i = 0; i < itemCount; i++) {
      const item = this.generateValue(schema.items);
      if (item !== null && item !== undefined) {
        array.push(item);
      }
    }
    
    return array;
  }
  
  /**
   * Generate from anyOf union
   */
  generateFromUnion(schema) {
    if (!schema.anyOf || schema.anyOf.length === 0) {
      return null;
    }
    
    // Randomly select one of the union types
    const selectedSchema = schema.anyOf[Math.floor(this.random() * schema.anyOf.length)];
    return this.generateValue(selectedSchema);
  }
  
  /**
   * Generate from enum values
   */
  generateFromEnum(enumValues) {
    if (!enumValues || enumValues.length === 0) {
      return '';
    }
    
    return enumValues[Math.floor(this.random() * enumValues.length)];
  }
  
  /**
   * Generate formatted string (email, uri, date-time, uuid, etc.)
   */
  generateFormattedString(format, schema) {
    switch (format) {
      case 'email':
        return this.generateEmail();
      case 'uri':
      case 'url':
        return this.generateUrl();
      case 'date-time':
        return this.generateDateTime();
      case 'date':
        return this.generateDate();
      case 'time':
        return this.generateTime();
      case 'uuid':
        return this.generateUuid();
      case 'ipv4':
        return this.generateIpv4();
      case 'ipv6':
        return this.generateIpv6();
      case 'hostname':
        return this.generateHostname();
      default:
        // Fallback to regular string generation
        return this.generateString({ ...schema, format: null });
    }
  }
  
  /**
   * Generate email address
   */
  generateEmail() {
    const usernames = ['john', 'jane', 'alice', 'bob', 'charlie', 'david', 'emma', 'frank'];
    const domains = ['example.com', 'test.org', 'mail.net', 'email.io', 'demo.co'];
    
    const username = usernames[Math.floor(this.random() * usernames.length)];
    const number = Math.floor(this.random() * 1000);
    const domain = domains[Math.floor(this.random() * domains.length)];
    
    return `${username}${number}@${domain}`;
  }
  
  /**
   * Generate URL
   */
  generateUrl() {
    const protocols = ['http', 'https'];
    const domains = ['example.com', 'test.org', 'demo.io', 'sample.net'];
    const paths = ['', '/api', '/data', '/users', '/products', '/about'];
    
    const protocol = protocols[Math.floor(this.random() * protocols.length)];
    const domain = domains[Math.floor(this.random() * domains.length)];
    const path = paths[Math.floor(this.random() * paths.length)];
    
    return `${protocol}://${domain}${path}`;
  }
  
  /**
   * Generate ISO date-time string
   */
  generateDateTime() {
    const year = 2020 + Math.floor(this.random() * 5);
    const month = Math.floor(this.random() * 12) + 1;
    const day = Math.floor(this.random() * 28) + 1;
    const hour = Math.floor(this.random() * 24);
    const minute = Math.floor(this.random() * 60);
    const second = Math.floor(this.random() * 60);
    
    return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}T` +
           `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:${String(second).padStart(2, '0')}.000Z`;
  }
  
  /**
   * Generate ISO date string
   */
  generateDate() {
    const year = 2020 + Math.floor(this.random() * 5);
    const month = Math.floor(this.random() * 12) + 1;
    const day = Math.floor(this.random() * 28) + 1;
    
    return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  }
  
  /**
   * Generate time string
   */
  generateTime() {
    const hour = Math.floor(this.random() * 24);
    const minute = Math.floor(this.random() * 60);
    const second = Math.floor(this.random() * 60);
    
    return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}:${String(second).padStart(2, '0')}`;
  }
  
  /**
   * Generate UUID v4
   */
  generateUuid() {
    const hex = '0123456789abcdef';
    let uuid = '';
    
    for (let i = 0; i < 36; i++) {
      if (i === 8 || i === 13 || i === 18 || i === 23) {
        uuid += '-';
      } else if (i === 14) {
        uuid += '4'; // Version 4
      } else if (i === 19) {
        uuid += hex[Math.floor(this.random() * 4) + 8]; // Variant
      } else {
        uuid += hex[Math.floor(this.random() * 16)];
      }
    }
    
    return uuid;
  }
  
  /**
   * Generate IPv4 address
   */
  generateIpv4() {
    const octets = [];
    for (let i = 0; i < 4; i++) {
      octets.push(Math.floor(this.random() * 256));
    }
    return octets.join('.');
  }
  
  /**
   * Generate IPv6 address
   */
  generateIpv6() {
    const hex = '0123456789abcdef';
    const groups = [];
    
    for (let i = 0; i < 8; i++) {
      let group = '';
      for (let j = 0; j < 4; j++) {
        group += hex[Math.floor(this.random() * 16)];
      }
      groups.push(group);
    }
    
    return groups.join(':');
  }
  
  /**
   * Generate hostname
   */
  generateHostname() {
    const prefixes = ['server', 'host', 'node', 'web', 'app', 'db', 'api'];
    const suffixes = ['example.com', 'local', 'internal', 'test.net'];
    
    const prefix = prefixes[Math.floor(this.random() * prefixes.length)];
    const number = Math.floor(this.random() * 100);
    const suffix = suffixes[Math.floor(this.random() * suffixes.length)];
    
    return `${prefix}${number}.${suffix}`;
  }
  
  /**
   * Generate random string of given length
   */
  generateRandomString(length) {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let result = '';
    
    for (let i = 0; i < length; i++) {
      result += chars[Math.floor(this.random() * chars.length)];
    }
    
    return result;
  }
  
  /**
   * Validate schema for common issues
   */
  validateSchema(schema) {
    const errors = [];
    this.validateSchemaRecursive(schema, '', errors);
    return errors;
  }
  
  /**
   * Recursively validate schema
   */
  validateSchemaRecursive(schema, path, errors) {
    if (!schema) return;
    
    // Validate string constraints
    if (schema.type === 'string') {
      if (schema.minLength !== undefined && schema.maxLength !== undefined) {
        if (schema.minLength > schema.maxLength) {
          errors.push(`${path}: minLength (${schema.minLength}) cannot be greater than maxLength (${schema.maxLength})`);
        }
      }
      if (schema.minLength !== undefined && schema.minLength < 0) {
        errors.push(`${path}: minLength cannot be negative`);
      }
    }
    
    // Validate numeric constraints
    if (schema.type === 'integer' || schema.type === 'number') {
      if (schema.minimum !== undefined && schema.maximum !== undefined) {
        if (schema.minimum > schema.maximum) {
          errors.push(`${path}: minimum (${schema.minimum}) cannot be greater than maximum (${schema.maximum})`);
        }
      }
      if (schema.multipleOf !== undefined && schema.multipleOf <= 0) {
        errors.push(`${path}: multipleOf must be greater than 0`);
      }
    }
    
    // Validate array constraints
    if (schema.type === 'array') {
      if (schema.minItems !== undefined && schema.maxItems !== undefined) {
        if (schema.minItems > schema.maxItems) {
          errors.push(`${path}: minItems (${schema.minItems}) cannot be greater than maxItems (${schema.maxItems})`);
        }
      }
      if (schema.minItems !== undefined && schema.minItems < 0) {
        errors.push(`${path}: minItems cannot be negative`);
      }
      
      // Validate items schema
      if (schema.items) {
        this.validateSchemaRecursive(schema.items, `${path}.items`, errors);
      }
    }
    
    // Validate object properties
    if (schema.type === 'object' && schema.properties) {
      for (const [key, propSchema] of Object.entries(schema.properties)) {
        this.validateSchemaRecursive(propSchema, `${path}.${key}`, errors);
      }
    }
    
    // Validate histogram
    if (schema.histogram) {
      if (schema.histogram.minValue > schema.histogram.maxValue) {
        errors.push(`${path}: histogram minValue (${schema.histogram.minValue}) cannot be greater than maxValue (${schema.histogram.maxValue})`);
      }
    }
    
    // Validate anyOf
    if (schema.anyOf) {
      for (let i = 0; i < schema.anyOf.length; i++) {
        this.validateSchemaRecursive(schema.anyOf[i], `${path}.anyOf[${i}]`, errors);
      }
    }
  }
}

export default DocumentGenerator;