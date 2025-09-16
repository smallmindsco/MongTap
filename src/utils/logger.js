/**
 * Logger module - First-class logging as per design guidelines
 * Implements configurable logging levels with no third-party dependencies
 */

const LOG_LEVELS = {
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3,
  TRACE: 4,
};

const LOG_COLORS = {
  ERROR: '\x1b[31m', // Red
  WARN: '\x1b[33m',  // Yellow
  INFO: '\x1b[36m',  // Cyan
  DEBUG: '\x1b[90m', // Gray
  TRACE: '\x1b[35m', // Magenta
  RESET: '\x1b[0m',
};

class Logger {
  constructor(module = 'MongTap', options = {}) {
    this.module = module;
    this.level = options.level || process.env.LOG_LEVEL || 'INFO';
    this.enabled = options.enabled !== false;
    this.useColors = options.colors !== false && process.stdout.isTTY;
    this.logToFile = options.file || null;
    this.timestamps = options.timestamps !== false;
  }

  setLevel(level) {
    if (LOG_LEVELS[level] !== undefined) {
      this.level = level;
    }
  }

  enable() {
    this.enabled = true;
  }

  disable() {
    this.enabled = false;
  }

  _shouldLog(level) {
    return this.enabled && LOG_LEVELS[level] <= LOG_LEVELS[this.level];
  }

  _formatMessage(level, message, meta) {
    const timestamp = this.timestamps ? new Date().toISOString() : '';
    const levelStr = level.padEnd(5);
    const moduleStr = `[${this.module}]`;
    
    let formatted = '';
    
    if (this.useColors) {
      formatted = `${LOG_COLORS[level]}${levelStr}${LOG_COLORS.RESET}`;
    } else {
      formatted = levelStr;
    }
    
    if (timestamp) {
      formatted = `${timestamp} ${formatted}`;
    }
    
    formatted += ` ${moduleStr} ${message}`;
    
    if (meta && Object.keys(meta).length > 0) {
      formatted += ` ${JSON.stringify(meta)}`;
    }
    
    return formatted;
  }

  _log(level, message, meta) {
    if (!this._shouldLog(level)) {
      return;
    }

    const formatted = this._formatMessage(level, message, meta);
    
    // Output to console
    if (level === 'ERROR') {
      console.error(formatted);
    } else if (level === 'WARN') {
      console.warn(formatted);
    } else {
      console.error(formatted);
    }
    
    // TODO: Add file logging when needed
    if (this.logToFile) {
      // Will implement file logging without third-party dependencies
    }
  }

  error(message, meta) {
    this._log('ERROR', message, meta);
  }

  warn(message, meta) {
    this._log('WARN', message, meta);
  }

  info(message, meta) {
    this._log('INFO', message, meta);
  }

  debug(message, meta) {
    this._log('DEBUG', message, meta);
  }

  trace(message, meta) {
    this._log('TRACE', message, meta);
  }

  // Create a child logger with a specific module name
  child(module) {
    return new Logger(`${this.module}:${module}`, {
      level: this.level,
      enabled: this.enabled,
      colors: this.useColors,
      file: this.logToFile,
      timestamps: this.timestamps,
    });
  }

  // Performance logging helper
  startTimer(label) {
    const start = process.hrtime.bigint();
    return {
      end: (message, meta = {}) => {
        const end = process.hrtime.bigint();
        const duration = Number(end - start) / 1000000; // Convert to milliseconds
        this.debug(`${message} (${duration.toFixed(2)}ms)`, { ...meta, duration });
      },
    };
  }
}

// Create singleton instance for default export
const defaultLogger = new Logger();

// Export both the class and default instance
export { Logger, defaultLogger as default };