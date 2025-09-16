/**
 * Shared configuration loader for MongTap
 * Loads configuration from mongtap.config.json with defaults
 */

import { readFileSync, existsSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Default configuration
const defaultConfig = {
  storage: {
    modelsBasePath: './mcp-models',
    defaultDatabase: 'mcp',
    databases: {
      mcp: {
        description: 'MCP-created models that appear as MongoDB collections',
        path: 'mcp'
      },
      trained: {
        description: 'Models trained from existing data',
        path: 'trained'
      },
      welldb: {
        description: 'WellDB database models',
        path: 'welldb-models'
      }
    }
  },
  server: {
    defaultPort: 27017,
    host: 'localhost',
    enableAutoTrain: true,
    trainThreshold: 100
  },
  generation: {
    defaultSeed: null,
    defaultEntropy: null,
    maxDocuments: 10000
  },
  logging: {
    level: 'info',
    suppressStdio: true
  }
};

/**
 * Load configuration from file or use defaults
 */
function loadConfig() {
  let config = JSON.parse(JSON.stringify(defaultConfig)); // Deep clone
  
  // Try multiple possible config locations
  const configPaths = [
    path.join(__dirname, '../../mongtap.config.json'),
    path.join(process.cwd(), 'mongtap.config.json'),
    path.join(__dirname, '../../../mongtap.config.json')
  ];
  
  let configLoaded = false;
  for (const configPath of configPaths) {
    if (existsSync(configPath)) {
      try {
        const configData = readFileSync(configPath, 'utf8');
        const userConfig = JSON.parse(configData);
        
        // Deep merge user config with defaults
        config = deepMerge(config, userConfig);
        configLoaded = true;
        break;
      } catch (err) {
        console.error(`Error loading config from ${configPath}:`, err.message);
      }
    }
  }
  
  // Resolve relative paths to absolute
  if (!path.isAbsolute(config.storage.modelsBasePath)) {
    config.storage.modelsBasePath = path.join(__dirname, '../..', config.storage.modelsBasePath);
  }
  
  // Add computed properties
  config.storage.defaultModelsPath = path.join(config.storage.modelsBasePath, config.storage.defaultDatabase);
  config.storage.trainedModelsPath = path.join(config.storage.modelsBasePath, config.storage.databases.trained?.path || 'trained');
  
  return config;
}

/**
 * Deep merge two objects
 */
function deepMerge(target, source) {
  const result = { ...target };
  
  for (const key in source) {
    if (source.hasOwnProperty(key)) {
      if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
        result[key] = deepMerge(result[key] || {}, source[key]);
      } else {
        result[key] = source[key];
      }
    }
  }
  
  return result;
}

// Load config once and export
const config = loadConfig();

export { config, loadConfig };
export default config;