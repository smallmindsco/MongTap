/**
 * Schema Generator Module
 * Port of DataFlood C# SchemaGenerator.cs
 * Generates DataFlood schemas from folders containing JSON/CSV files
 */

import { SchemaInferrer } from './inferrer.js';
import { DataFloodModel } from '../models/DataFloodModel.js';
import { readFileSync, readdirSync, statSync } from 'fs';
import { join, extname, basename } from 'path';
import logger from '../../utils/logger.js';

const log = logger.child('SchemaGenerator');

export class SchemaGenerator {
  constructor() {
    this.processedJsonFileCount = 0;
    this.processedCsvFileCount = 0;
    this.schemaInferrer = new SchemaInferrer({
      inferStringModels: true,
      inferHistograms: true,
      detectRelationships: true,
      detectFormats: true,
      detectPatterns: true,
    });
  }
  
  /**
   * Generate schema from a folder containing JSON/CSV files
   * Matches C# GenerateSchema method
   * @param {string} folderPath - Path to folder containing data files
   * @returns {DataFloodModel} - Generated schema
   */
  generateSchema(folderPath) {
    // Reset counters
    this.processedJsonFileCount = 0;
    this.processedCsvFileCount = 0;
    
    // Find all JSON and CSV files recursively
    const jsonFiles = this.findFiles(folderPath, '.json');
    const csvFiles = this.findFiles(folderPath, '.csv');
    
    if (jsonFiles.length === 0 && csvFiles.length === 0) {
      log.warn('No JSON or CSV files found in the specified directory.');
      return new DataFloodModel({
        $schema: 'https://smallminds.co/DataFlood/schema#',
        type: 'object',
      });
    }
    
    const allDocuments = [];
    
    // Process JSON files
    for (const file of jsonFiles) {
      try {
        const content = readFileSync(file, 'utf8');
        
        // Skip markdown files mistakenly saved as .json
        if (content.trim().startsWith('#')) {
          log.debug(`Skipping markdown file: ${basename(file)}`);
          continue;
        }
        
        const jsonData = JSON.parse(content);
        
        // If it's an array, add each element as a document
        if (Array.isArray(jsonData)) {
          allDocuments.push(...jsonData);
        } else {
          allDocuments.push(jsonData);
        }
        
        this.processedJsonFileCount++;
        log.debug(`Processed JSON file: ${basename(file)}`);
      } catch (error) {
        log.warn(`Failed to parse JSON file ${basename(file)}: ${error.message}`);
      }
    }
    
    // Process CSV files
    for (const file of csvFiles) {
      try {
        const csvObjects = this.parseCsvToJson(file);
        allDocuments.push(...csvObjects);
        this.processedCsvFileCount++;
        log.debug(`Processed CSV file: ${basename(file)}`);
      } catch (error) {
        log.warn(`Error processing CSV file ${basename(file)}: ${error.message}`);
      }
    }
    
    if (allDocuments.length === 0) {
      throw new Error('No valid JSON or CSV documents found.');
    }
    
    log.info(`Processed ${this.processedJsonFileCount} JSON files and ${this.processedCsvFileCount} CSV files`);
    log.info(`Total documents: ${allDocuments.length}`);
    
    // Infer schema from all documents
    const schema = this.schemaInferrer.inferSchema(allDocuments);
    
    // Ensure schema has DataFlood $schema property
    if (!schema.$schema) {
      schema.$schema = 'https://smallminds.co/DataFlood/schema#';
    }
    
    return schema;
  }
  
  /**
   * Generate schema from multiple folders
   * @param {string[]} folderPaths - Array of folder paths
   * @returns {DataFloodModel} - Merged schema
   */
  generateSchemaFromMultipleFolders(folderPaths) {
    const schemas = [];
    
    for (const folderPath of folderPaths) {
      try {
        const schema = this.generateSchema(folderPath);
        schemas.push(schema);
      } catch (error) {
        log.warn(`Failed to generate schema from ${folderPath}: ${error.message}`);
      }
    }
    
    if (schemas.length === 0) {
      throw new Error('No schemas could be generated from any folder.');
    }
    
    // Merge all schemas
    return this.mergeSchemas(schemas);
  }
  
  /**
   * Recursively find files with a specific extension
   * @param {string} dir - Directory to search
   * @param {string} ext - File extension (e.g., '.json')
   * @returns {string[]} - Array of file paths
   */
  findFiles(dir, ext, fileList = []) {
    try {
      const files = readdirSync(dir);
      
      for (const file of files) {
        // Skip hidden files and directories
        if (file.startsWith('.')) continue;
        
        const filePath = join(dir, file);
        const stat = statSync(filePath);
        
        if (stat.isDirectory()) {
          this.findFiles(filePath, ext, fileList);
        } else if (extname(file).toLowerCase() === ext) {
          fileList.push(filePath);
        }
      }
    } catch (error) {
      log.warn(`Error reading directory ${dir}: ${error.message}`);
    }
    
    return fileList;
  }
  
  /**
   * Parse CSV file to JSON objects
   * Simplified CSV parser - production would use a library
   * @param {string} filePath - Path to CSV file
   * @returns {Object[]} - Array of JSON objects
   */
  parseCsvToJson(filePath) {
    const content = readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());
    
    if (lines.length < 2) {
      return [];
    }
    
    // Parse headers
    const headers = this.parseCsvLine(lines[0]);
    const objects = [];
    
    // Parse data rows
    for (let i = 1; i < lines.length; i++) {
      const values = this.parseCsvLine(lines[i]);
      
      if (values.length === headers.length) {
        const obj = {};
        for (let j = 0; j < headers.length; j++) {
          const value = values[j];
          // Try to parse as number
          if (/^-?\d+(\.\d+)?$/.test(value)) {
            obj[headers[j]] = parseFloat(value);
          } else if (value.toLowerCase() === 'true' || value.toLowerCase() === 'false') {
            obj[headers[j]] = value.toLowerCase() === 'true';
          } else {
            obj[headers[j]] = value;
          }
        }
        objects.push(obj);
      }
    }
    
    return objects;
  }
  
  /**
   * Parse a single CSV line handling quoted values
   * @param {string} line - CSV line
   * @returns {string[]} - Array of values
   */
  parseCsvLine(line) {
    const values = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          // Escaped quote
          current += '"';
          i++;
        } else {
          // Toggle quote state
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        values.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    
    // Add last value
    if (current || line.endsWith(',')) {
      values.push(current.trim());
    }
    
    return values;
  }
  
  /**
   * Merge multiple schemas into one
   * @param {DataFloodModel[]} schemas - Array of schemas to merge
   * @returns {DataFloodModel} - Merged schema
   */
  mergeSchemas(schemas) {
    if (schemas.length === 1) {
      return schemas[0];
    }
    
    // Start with first schema
    let merged = schemas[0];
    
    // Merge remaining schemas
    for (let i = 1; i < schemas.length; i++) {
      merged = this.schemaInferrer.mergeSchemas(merged, schemas[i]);
    }
    
    return merged;
  }
  
  /**
   * Get processing statistics
   * @returns {Object} - Statistics object
   */
  getStatistics() {
    return {
      processedJsonFileCount: this.processedJsonFileCount,
      processedCsvFileCount: this.processedCsvFileCount,
      totalFilesProcessed: this.processedJsonFileCount + this.processedCsvFileCount,
    };
  }
}

// Export default instance
export default SchemaGenerator;