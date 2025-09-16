/**
 * Configuration system for MongTap
 * Manages all configuration without third-party dependencies
 */

import { readFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import logger from './logger.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const log = logger.child('Config');

class Config {
  constructor() {
    this.config = this.loadDefaults();
    this.loadFromFile();
    this.loadFromEnv();
    log.debug('Configuration loaded', { config: this.config });
  }

  loadDefaults() {
    return {
      mcp: {
        name: 'mongtap',
        version: '0.1.0',
        description: 'AI-powered MongoDB server with DataFlood models',
        port: 3000,
        host: '0.0.0.0',
      },
      dataflood: {
        modelPath: './models',
        tempPath: './temp',
        testDataPath: './test-data',
        generationOptions: {
          stringModelDepth: 3,
          histogramBins: 20,
          patternDetection: true,
          defaultSeed: null, // null for random, number for deterministic
        },
      },
      welldb: {
        dataPath: './welldb-data',
        portRange: [27018, 27099],
        defaultPort: 27017,
        cacheSize: 100, // MB
        workerThreads: 4,
        maxConnections: 1000,
      },
      limits: {
        maxServers: 10,
        maxModels: 100,
        maxSampleSize: 10000,
        serverTimeout: 3600000, // 1 hour in ms
        generationTimeout: 30000, // 30 seconds
      },
      logging: {
        level: 'INFO',
        file: null,
        colors: true,
        timestamps: true,
      },
      testing: {
        enabled: false,
        fixturesPath: './test/fixtures',
      },
    };
  }

  loadFromFile() {
    const configPaths = [
      join(process.cwd(), 'mongtap.config.json'),
      join(process.cwd(), '.mongtaprc.json'),
      join(__dirname, '../../mongtap.config.json'),
    ];

    for (const configPath of configPaths) {
      if (existsSync(configPath)) {
        try {
          const fileContent = readFileSync(configPath, 'utf8');
          const fileConfig = JSON.parse(fileContent);
          this.config = this.deepMerge(this.config, fileConfig);
          log.info(`Configuration loaded from ${configPath}`);
          break;
        } catch (error) {
          log.error(`Failed to load config from ${configPath}`, { error: error.message });
        }
      }
    }
  }

  loadFromEnv() {
    // MCP configuration
    if (process.env.MCP_PORT) {
      this.config.mcp.port = parseInt(process.env.MCP_PORT, 10);
    }
    if (process.env.MCP_HOST) {
      this.config.mcp.host = process.env.MCP_HOST;
    }

    // DataFlood configuration
    if (process.env.DATAFLOOD_MODEL_PATH) {
      this.config.dataflood.modelPath = process.env.DATAFLOOD_MODEL_PATH;
    }
    if (process.env.DATAFLOOD_SEED) {
      this.config.dataflood.generationOptions.defaultSeed = parseInt(process.env.DATAFLOOD_SEED, 10);
    }

    // WellDB configuration
    if (process.env.WELLDB_PORT) {
      this.config.welldb.defaultPort = parseInt(process.env.WELLDB_PORT, 10);
    }
    if (process.env.WELLDB_CACHE_SIZE) {
      this.config.welldb.cacheSize = parseInt(process.env.WELLDB_CACHE_SIZE, 10);
    }

    // Logging configuration
    if (process.env.LOG_LEVEL) {
      this.config.logging.level = process.env.LOG_LEVEL;
    }
    if (process.env.LOG_FILE) {
      this.config.logging.file = process.env.LOG_FILE;
    }

    // Testing mode
    if (process.env.NODE_ENV === 'test') {
      this.config.testing.enabled = true;
    }
  }

  deepMerge(target, source) {
    const output = { ...target };
    
    for (const key in source) {
      if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
        if (target[key] && typeof target[key] === 'object' && !Array.isArray(target[key])) {
          output[key] = this.deepMerge(target[key], source[key]);
        } else {
          output[key] = source[key];
        }
      } else {
        output[key] = source[key];
      }
    }
    
    return output;
  }

  get(path) {
    const keys = path.split('.');
    let value = this.config;
    
    for (const key of keys) {
      if (value && typeof value === 'object' && key in value) {
        value = value[key];
      } else {
        return undefined;
      }
    }
    
    return value;
  }

  set(path, value) {
    const keys = path.split('.');
    let target = this.config;
    
    for (let i = 0; i < keys.length - 1; i++) {
      const key = keys[i];
      if (!target[key] || typeof target[key] !== 'object') {
        target[key] = {};
      }
      target = target[key];
    }
    
    target[keys[keys.length - 1]] = value;
    log.debug(`Configuration updated: ${path} = ${value}`);
  }

  getAll() {
    return { ...this.config };
  }

  // Validate configuration
  validate() {
    const errors = [];

    // Validate ports
    if (this.config.mcp.port < 1 || this.config.mcp.port > 65535) {
      errors.push('MCP port must be between 1 and 65535');
    }
    if (this.config.welldb.defaultPort < 1 || this.config.welldb.defaultPort > 65535) {
      errors.push('WellDB port must be between 1 and 65535');
    }

    // Validate paths exist or can be created
    // TODO: Add path validation when needed

    // Validate limits
    if (this.config.limits.maxSampleSize < 1) {
      errors.push('Max sample size must be positive');
    }

    if (errors.length > 0) {
      throw new Error(`Configuration validation failed:\n${errors.join('\n')}`);
    }

    return true;
  }
}

// Create singleton instance
const config = new Config();

export default config;