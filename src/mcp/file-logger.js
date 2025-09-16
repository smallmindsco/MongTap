/**
 * File-based logger for MCP server
 * Writes all logs to files instead of console
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export class FileLogger {
    constructor(mode = 'stdio') {
        this.mode = mode;
        
        // Log directory
        this.logDir = path.join(__dirname, '../../logs');
        
        // Create log directory if it doesn't exist
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
        
        // Create timestamp with YYYYMMDD-HHMMSS format for easy sorting and uniqueness
        const now = new Date();
        const timestamp = [
            now.getFullYear(),
            String(now.getMonth() + 1).padStart(2, '0'),
            String(now.getDate()).padStart(2, '0'),
            '-',
            String(now.getHours()).padStart(2, '0'),
            String(now.getMinutes()).padStart(2, '0'),
            String(now.getSeconds()).padStart(2, '0')
        ].join('');
        
        this.logFile = path.join(this.logDir, `mongtap-${timestamp}.log`);
        this.errorFile = path.join(this.logDir, `mongtap-error-${timestamp}.log`);
        
        // Create log streams
        this.logStream = fs.createWriteStream(this.logFile, { flags: 'a' });
        this.errorStream = fs.createWriteStream(this.errorFile, { flags: 'a' });
        
        // Write startup message
        this.writeLog('info', `MongTap MCP Server started in ${mode} mode`);
    }
    
    writeLog(level, ...args) {
        const timestamp = new Date().toISOString();
        const message = args.map(arg => 
            typeof arg === 'object' ? JSON.stringify(arg) : String(arg)
        ).join(' ');
        
        const logLine = `[${timestamp}] [${level.toUpperCase()}] ${message}\n`;
        
        if (level === 'error') {
            this.errorStream.write(logLine);
        } else {
            this.logStream.write(logLine);
        }
    }
    
    debug(...args) {
        if (this.mode === 'stdio') return; // Complete silence in stdio mode
        this.writeLog('debug', ...args);
    }
    
    info(...args) {
        if (this.mode === 'stdio') return; // Complete silence in stdio mode
        this.writeLog('info', ...args);
    }
    
    warn(...args) {
        if (this.mode === 'stdio') return; // Complete silence in stdio mode
        this.writeLog('warn', ...args);
    }
    
    error(...args) {
        if (this.mode === 'stdio') return; // Complete silence in stdio mode
        this.writeLog('error', ...args);
    }
    
    close() {
        this.logStream.end();
        this.errorStream.end();
    }
}

export default FileLogger;