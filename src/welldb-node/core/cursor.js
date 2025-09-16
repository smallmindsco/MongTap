/**
 * MongoDB Cursor Implementation with DataFlood Integration
 * 
 * Provides streaming access to large result sets by:
 * - Generating documents in batches from DataFlood models
 * - Maintaining cursor state across multiple getMore operations
 * - Supporting cursor options like batchSize, limit, and timeout
 */

import { EventEmitter } from 'events';
import crypto from 'crypto';

/**
 * Cursor for streaming query results
 */
export class Cursor extends EventEmitter {
    constructor(options = {}) {
        super();
        
        // Cursor identification
        this.id = this.generateCursorId();
        this.namespace = options.namespace || 'test.collection';
        
        // Query parameters
        this.query = options.query || {};
        this.projection = options.projection || null;
        this.sort = options.sort || null;
        this.skip = options.skip || 0;
        this.limit = options.limit || 0; // 0 means no limit
        
        // Cursor options
        this.batchSize = options.batchSize || 101;
        this.timeout = options.timeout || 600000; // 10 minutes default
        this.tailable = options.tailable || false;
        this.awaitData = options.awaitData || false;
        
        // Internal state
        this.collection = options.collection;
        this.queryEngine = options.queryEngine;
        this.position = 0;
        this.documentsSent = 0;
        this.isExhausted = false;
        this.isClosed = false;
        this.createdAt = new Date();
        this.lastAccessed = new Date();
        
        // Document buffer for efficient batch delivery
        this.buffer = [];
        this.bufferSize = options.bufferSize || 1000;
        
        // Statistics
        this.stats = {
            documentsReturned: 0,
            batchesReturned: 0,
            generationCalls: 0,
            totalGenerationTime: 0
        };
        
        // Set up timeout if specified
        if (this.timeout > 0) {
            this.setupTimeout();
        }
        
        // Logging
        this.logger = options.logger || this.createDefaultLogger();
    }
    
    createDefaultLogger() {
        return {
            debug: () => {},
            info: console.error,
            warn: console.warn,
            error: console.error
        };
    }
    
    /**
     * Generate a unique cursor ID
     */
    generateCursorId() {
        // Generate a BigInt cursor ID
        const buffer = crypto.randomBytes(8);
        return buffer.readBigUInt64BE();
    }
    
    /**
     * Get the next batch of documents
     */
    async getNextBatch(requestedBatchSize) {
        if (this.isClosed) {
            throw new Error('Cursor is closed');
        }
        
        if (this.isExhausted) {
            return [];
        }
        
        this.lastAccessed = new Date();
        
        const batchSize = requestedBatchSize || this.batchSize;
        const batch = [];
        
        try {
            // First, drain any buffered documents
            while (this.buffer.length > 0 && batch.length < batchSize) {
                batch.push(this.buffer.shift());
                this.documentsSent++;
                
                // Check if we've hit the limit
                if (this.limit > 0 && this.documentsSent >= this.limit) {
                    this.isExhausted = true;
                    break;
                }
            }
            
            // If we need more documents and haven't hit limit, generate them
            if (batch.length < batchSize && !this.isExhausted) {
                const needed = Math.min(
                    batchSize - batch.length,
                    this.bufferSize
                );
                
                // Account for limit if set
                const toGenerate = this.limit > 0 
                    ? Math.min(needed, this.limit - this.documentsSent)
                    : needed;
                
                if (toGenerate > 0) {
                    const genStart = Date.now();
                    
                    // Generate documents using query engine
                    const generated = await this.generateDocuments(toGenerate);
                    
                    const genTime = Date.now() - genStart;
                    this.stats.generationCalls++;
                    this.stats.totalGenerationTime += genTime;
                    
                    this.logger.debug(
                        `Cursor ${this.id} generated ${generated.length} documents in ${genTime}ms`
                    );
                    
                    // Add to batch and buffer
                    for (const doc of generated) {
                        if (batch.length < batchSize) {
                            batch.push(doc);
                            this.documentsSent++;
                        } else {
                            this.buffer.push(doc);
                        }
                        
                        // Check limit
                        if (this.limit > 0 && this.documentsSent >= this.limit) {
                            this.isExhausted = true;
                            break;
                        }
                    }
                }
            }
            
            // Update statistics
            this.stats.documentsReturned += batch.length;
            this.stats.batchesReturned++;
            
            // Check if cursor is exhausted
            if (batch.length === 0 || 
                (this.limit > 0 && this.documentsSent >= this.limit)) {
                this.isExhausted = true;
            }
            
            this.logger.debug(
                `Cursor ${this.id} returned batch of ${batch.length} documents ` +
                `(total: ${this.stats.documentsReturned})`
            );
            
            return batch;
            
        } catch (error) {
            this.logger.error(`Cursor ${this.id} error:`, error);
            this.close();
            throw error;
        }
    }
    
    /**
     * Generate documents using the query engine
     */
    async generateDocuments(count) {
        if (!this.queryEngine || !this.collection) {
            // Fallback: return empty array if no engine available
            return [];
        }
        
        // Use collection's find method directly
        const documents = await this.collection.find(
            this.query,
            {
                skip: this.position,
                limit: count,
                sort: this.sort,
                projection: this.projection
            }
        );
        
        this.position += documents.length;
        return documents;
    }
    
    /**
     * Get cursor information
     */
    getInfo() {
        return {
            id: this.id,
            ns: this.namespace,
            firstBatch: [], // Will be populated by first getMore
            isExhausted: this.isExhausted,
            documentsReturned: this.stats.documentsReturned,
            position: this.position
        };
    }
    
    /**
     * Kill/close the cursor
     */
    close() {
        if (this.isClosed) {
            return;
        }
        
        this.isClosed = true;
        this.isExhausted = true;
        this.buffer = [];
        
        if (this.timeoutTimer) {
            clearTimeout(this.timeoutTimer);
        }
        
        this.logger.info(`Cursor ${this.id} closed after returning ${this.stats.documentsReturned} documents`);
        this.emit('close', this.stats);
    }
    
    /**
     * Set up cursor timeout
     */
    setupTimeout() {
        const checkTimeout = () => {
            const idle = Date.now() - this.lastAccessed.getTime();
            
            if (idle >= this.timeout) {
                this.logger.info(`Cursor ${this.id} timed out after ${idle}ms`);
                this.close();
            } else {
                // Check again later
                this.timeoutTimer = setTimeout(checkTimeout, Math.min(60000, this.timeout));
            }
        };
        
        this.timeoutTimer = setTimeout(checkTimeout, Math.min(60000, this.timeout));
    }
    
    /**
     * Check if cursor is still valid
     */
    isValid() {
        return !this.isClosed && !this.isExhausted;
    }
    
    /**
     * Get cursor statistics
     */
    getStats() {
        return {
            ...this.stats,
            isExhausted: this.isExhausted,
            isClosed: this.isClosed,
            bufferSize: this.buffer.length,
            age: Date.now() - this.createdAt.getTime(),
            idleTime: Date.now() - this.lastAccessed.getTime(),
            averageGenerationTime: this.stats.generationCalls > 0 
                ? this.stats.totalGenerationTime / this.stats.generationCalls 
                : 0
        };
    }
}

/**
 * Cursor Manager for tracking active cursors
 */
export class CursorManager {
    constructor(options = {}) {
        this.cursors = new Map();
        this.maxCursors = options.maxCursors || 1000;
        this.defaultTimeout = options.defaultTimeout || 600000;
        this.logger = options.logger || this.createDefaultLogger();
        
        // Statistics
        this.stats = {
            totalCreated: 0,
            totalClosed: 0,
            totalTimedOut: 0,
            totalDocumentsReturned: 0
        };
        
        // Periodic cleanup
        this.cleanupInterval = setInterval(() => {
            this.cleanup();
        }, 60000); // Every minute
    }
    
    createDefaultLogger() {
        return {
            debug: () => {},
            info: console.error,
            warn: console.warn,
            error: console.error
        };
    }
    
    /**
     * Create a new cursor
     */
    createCursor(options) {
        // Enforce max cursors limit
        while (this.cursors.size >= this.maxCursors) {
            // Remove oldest cursor
            const oldest = this.getOldestCursor();
            if (oldest) {
                oldest.close();
                this.cursors.delete(oldest.id);
            } else {
                break;
            }
        }
        
        const cursor = new Cursor({
            ...options,
            timeout: options.timeout || this.defaultTimeout,
            logger: this.logger
        });
        
        // Listen for cursor close
        cursor.on('close', (stats) => {
            this.stats.totalClosed++;
            this.stats.totalDocumentsReturned += stats.documentsReturned;
            this.cursors.delete(cursor.id);
        });
        
        this.cursors.set(cursor.id, cursor);
        this.stats.totalCreated++;
        
        this.logger.debug(`Created cursor ${cursor.id} for ${cursor.namespace}`);
        
        return cursor;
    }
    
    /**
     * Get a cursor by ID
     */
    getCursor(cursorId) {
        return this.cursors.get(cursorId);
    }
    
    /**
     * Close a cursor
     */
    closeCursor(cursorId) {
        const cursor = this.cursors.get(cursorId);
        if (cursor) {
            cursor.close();
            return true;
        }
        return false;
    }
    
    /**
     * Close multiple cursors
     */
    closeCursors(cursorIds) {
        const results = [];
        for (const id of cursorIds) {
            results.push(this.closeCursor(id));
        }
        return results;
    }
    
    /**
     * Get the oldest cursor
     */
    getOldestCursor() {
        let oldest = null;
        let oldestTime = Date.now();
        
        for (const cursor of this.cursors.values()) {
            if (cursor.createdAt.getTime() < oldestTime) {
                oldest = cursor;
                oldestTime = cursor.createdAt.getTime();
            }
        }
        
        return oldest;
    }
    
    /**
     * Clean up expired cursors
     */
    cleanup() {
        const now = Date.now();
        const toClose = [];
        
        for (const [id, cursor] of this.cursors) {
            // Check if cursor should be closed
            if (cursor.isClosed || 
                cursor.isExhausted ||
                (cursor.timeout > 0 && now - cursor.lastAccessed.getTime() > cursor.timeout)) {
                toClose.push(id);
            }
        }
        
        for (const id of toClose) {
            const cursor = this.cursors.get(id);
            if (cursor && !cursor.isClosed) {
                this.stats.totalTimedOut++;
                cursor.close();
            }
            this.cursors.delete(id);
        }
        
        if (toClose.length > 0) {
            this.logger.debug(`Cleaned up ${toClose.length} cursors`);
        }
    }
    
    /**
     * Get manager statistics
     */
    getStats() {
        return {
            ...this.stats,
            activeCursors: this.cursors.size,
            cursors: Array.from(this.cursors.values()).map(c => ({
                id: c.id,
                namespace: c.namespace,
                documentsReturned: c.stats.documentsReturned,
                isExhausted: c.isExhausted,
                age: Date.now() - c.createdAt.getTime()
            }))
        };
    }
    
    /**
     * Clear all cursors
     */
    clear() {
        for (const cursor of this.cursors.values()) {
            cursor.close();
        }
        this.cursors.clear();
    }
    
    /**
     * Destroy the manager
     */
    destroy() {
        this.clear();
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
        }
    }
}

export default { Cursor, CursorManager };