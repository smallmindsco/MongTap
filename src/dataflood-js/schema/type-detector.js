/**
 * Type Detection Module
 * Port of DataFlood C# type detection logic
 * Matches DataFlood's type detection exactly
 */

import logger from '../../utils/logger.js';

const log = logger.child('TypeDetector');

/**
 * Detect the JSON type of a value
 * Matches C# GetJsonType logic
 */
export function detectType(value) {
  if (value === null || value === undefined) {
    return 'null';
  }
  
  if (typeof value === 'boolean') {
    return 'boolean';
  }
  
  if (typeof value === 'string') {
    return 'string';
  }
  
  if (typeof value === 'number') {
    // Check if it's an integer
    if (Number.isInteger(value)) {
      return 'integer';
    }
    return 'number';
  }
  
  if (Array.isArray(value)) {
    return 'array';
  }
  
  if (typeof value === 'object') {
    return 'object';
  }
  
  // Fallback
  return 'string';
}

/**
 * Detect if all values in array are same type
 */
export function detectArrayItemType(items) {
  if (!items || items.length === 0) {
    return null;
  }
  
  const types = items.map(detectType);
  const uniqueTypes = [...new Set(types)];
  
  if (uniqueTypes.length === 1) {
    return uniqueTypes[0];
  }
  
  // Check if mix of integer and number - treat as number
  if (uniqueTypes.length === 2 && 
      uniqueTypes.includes('integer') && 
      uniqueTypes.includes('number')) {
    return 'number';
  }
  
  // Multiple types - would need anyOf
  return null;
}

/**
 * Detect format of a string value
 * Matches DataFlood's format detection patterns
 */
export function detectFormat(value) {
  if (typeof value !== 'string') {
    return null;
  }
  
  // Email pattern
  const emailPattern = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
  if (emailPattern.test(value)) {
    return 'email';
  }
  
  // URI/URL pattern
  const uriPattern = /^https?:\/\/[^\s/$.?#].[^\s]*$/i;
  if (uriPattern.test(value)) {
    return 'uri';
  }
  
  // UUID pattern
  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (uuidPattern.test(value)) {
    return 'uuid';
  }
  
  // ISO 8601 date-time pattern
  const dateTimePattern = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?(Z|[+-]\d{2}:\d{2})?$/;
  if (dateTimePattern.test(value)) {
    return 'date-time';
  }
  
  // Date only pattern
  const datePattern = /^\d{4}-\d{2}-\d{2}$/;
  if (datePattern.test(value)) {
    return 'date';
  }
  
  // Time only pattern
  const timePattern = /^\d{2}:\d{2}:\d{2}(\.\d{3})?$/;
  if (timePattern.test(value)) {
    return 'time';
  }
  
  // IPv4 pattern
  const ipv4Pattern = /^(\d{1,3}\.){3}\d{1,3}$/;
  if (ipv4Pattern.test(value)) {
    const parts = value.split('.');
    if (parts.every(part => parseInt(part) <= 255)) {
      return 'ipv4';
    }
  }
  
  // IPv6 pattern (simplified)
  const ipv6Pattern = /^([0-9a-f]{1,4}:){7}[0-9a-f]{1,4}$/i;
  if (ipv6Pattern.test(value)) {
    return 'ipv6';
  }
  
  return null;
}

/**
 * Detect format from multiple string samples
 * Returns format only if consistent across samples
 */
export function detectConsistentFormat(values) {
  if (!values || values.length === 0) {
    return null;
  }
  
  const formats = values.map(detectFormat);
  const uniqueFormats = [...new Set(formats)];
  
  // Only return format if all values have same format
  if (uniqueFormats.length === 1 && uniqueFormats[0] !== null) {
    return uniqueFormats[0];
  }
  
  return null;
}

/**
 * Check if values should be an enum
 * Based on DataFlood's enum detection logic
 */
export function shouldBeEnum(values, threshold = 0.5) {
  if (!values || values.length === 0) {
    return false;
  }
  
  const uniqueValues = [...new Set(values)];
  const uniqueRatio = uniqueValues.length / values.length;
  
  // If unique ratio is low and not too many unique values, suggest enum
  if (uniqueRatio < threshold && uniqueValues.length <= 20) {
    return true;
  }
  
  // Also check if all values are from a small set
  if (uniqueValues.length <= 5 && values.length >= 3) {
    return true;
  }
  
  return false;
}

/**
 * Analyze numeric values to determine constraints
 */
export function analyzeNumericConstraints(values) {
  if (!values || values.length === 0) {
    return {};
  }
  
  const numbers = values.filter(v => typeof v === 'number');
  if (numbers.length === 0) {
    return {};
  }
  
  const constraints = {
    minimum: Math.min(...numbers),
    maximum: Math.max(...numbers),
  };
  
  // Check for multipleOf pattern
  if (numbers.length > 1) {
    const diffs = [];
    for (let i = 1; i < numbers.length; i++) {
      const diff = Math.abs(numbers[i] - numbers[i-1]);
      if (diff > 0) {
        diffs.push(diff);
      }
    }
    
    if (diffs.length > 0) {
      // Find GCD of differences to detect multipleOf
      const gcd = (a, b) => b === 0 ? a : gcd(b, a % b);
      let commonDivisor = diffs[0];
      for (const diff of diffs) {
        commonDivisor = gcd(commonDivisor, diff);
      }
      
      if (commonDivisor > 1 && commonDivisor < constraints.maximum) {
        // Check if all numbers are multiples
        if (numbers.every(n => n % commonDivisor === 0)) {
          constraints.multipleOf = commonDivisor;
        }
      }
    }
  }
  
  return constraints;
}

/**
 * Analyze string values to determine constraints
 */
export function analyzeStringConstraints(values) {
  if (!values || values.length === 0) {
    return {};
  }
  
  const strings = values.filter(v => typeof v === 'string');
  if (strings.length === 0) {
    return {};
  }
  
  const lengths = strings.map(s => s.length);
  
  return {
    minLength: Math.min(...lengths),
    maxLength: Math.max(...lengths),
  };
}

/**
 * Detect if a string follows a pattern
 */
export function detectPattern(values) {
  if (!values || values.length < 3) {
    return null;
  }
  
  // Check for common patterns
  const patterns = [
    { regex: /^[A-Z]{2,4}-\d{3,5}$/, pattern: '^[A-Z]{2,4}-\\d{3,5}$' }, // Product codes
    { regex: /^[A-Z]\d{6}$/, pattern: '^[A-Z]\\d{6}$' }, // Serial numbers
    { regex: /^\d{3}-\d{3}-\d{4}$/, pattern: '^\\d{3}-\\d{3}-\\d{4}$' }, // Phone numbers
    { regex: /^[A-Z]{3}\d{3}$/, pattern: '^[A-Z]{3}\\d{3}$' }, // Flight codes
  ];
  
  for (const { regex, pattern } of patterns) {
    if (values.every(v => regex.test(v))) {
      return pattern;
    }
  }
  
  return null;
}

// Export all functions
export default {
  detectType,
  detectArrayItemType,
  detectFormat,
  detectConsistentFormat,
  shouldBeEnum,
  analyzeNumericConstraints,
  analyzeStringConstraints,
  detectPattern,
};