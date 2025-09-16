/**
 * MongoDB Session Management
 * Handles session tracking and transaction support
 */

import { EventEmitter } from 'events';
import crypto from 'crypto';

/**
 * Session states
 */
export const SessionState = {
    ACTIVE: 'active',
    IDLE: 'idle',
    TRANSACTION_IN_PROGRESS: 'transaction_in_progress',
    TRANSACTION_COMMITTED: 'transaction_committed',
    TRANSACTION_ABORTED: 'transaction_aborted',
    EXPIRED: 'expired',
    ENDED: 'ended'
};

/**
 * MongoDB Session
 */
export class Session extends EventEmitter {
    constructor(options = {}) {
        super();
        
        // Session identification
        this.id = options.id || this.generateSessionId();
        this.connectionId = options.connectionId;
        this.userId = options.userId || null;
        
        // Session state
        this.state = SessionState.ACTIVE;
        this.database = options.database || 'test';
        
        // Timing
        this.createdAt = new Date();
        this.lastActivity = new Date();
        this.timeout = options.timeout || 1800000; // 30 minutes default
        this.idleTimeout = options.idleTimeout || 600000; // 10 minutes default
        
        // Transaction support
        this.inTransaction = false;
        this.transactionId = null;
        this.transactionOperations = [];
        this.transactionStartTime = null;
        
        // Operation history
        this.operationHistory = [];
        this.maxHistorySize = options.maxHistorySize || 100;
        
        // Statistics
        this.stats = {
            operations: 0,
            transactions: 0,
            commits: 0,
            aborts: 0,
            errors: 0
        };
        
        // Logging
        this.logger = options.logger || this.createDefaultLogger();
        
        // Start timeout checker
        this.startTimeoutChecker();
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
     * Generate unique session ID
     */
    generateSessionId() {
        return crypto.randomBytes(16).toString('hex');
    }

    /**
     * Start timeout checker
     */
    startTimeoutChecker() {
        this.timeoutChecker = setInterval(() => {
            this.checkTimeout();
        }, 60000); // Check every minute
    }

    /**
     * Check for session timeout
     */
    checkTimeout() {
        if (this.state === SessionState.ENDED || this.state === SessionState.EXPIRED) {
            return;
        }
        
        const now = Date.now();
        const lastActivityTime = this.lastActivity.getTime();
        const createdTime = this.createdAt.getTime();
        
        // Check idle timeout
        if (this.state === SessionState.IDLE && (now - lastActivityTime) > this.idleTimeout) {
            this.expire('Idle timeout');
            return;
        }
        
        // Check absolute timeout
        if ((now - createdTime) > this.timeout) {
            this.expire('Session timeout');
            return;
        }
        
        // Set to idle if no recent activity
        if (this.state === SessionState.ACTIVE && (now - lastActivityTime) > 60000) {
            this.state = SessionState.IDLE;
            this.emit('idle');
        }
    }

    /**
     * Update activity timestamp
     */
    touch() {
        this.lastActivity = new Date();
        if (this.state === SessionState.IDLE) {
            this.state = SessionState.ACTIVE;
            this.emit('active');
        }
    }

    /**
     * Record an operation
     */
    recordOperation(operation) {
        this.touch();
        this.stats.operations++;
        
        const op = {
            timestamp: new Date(),
            operation: operation.type || 'unknown',
            collection: operation.collection,
            database: operation.database || this.database,
            success: operation.success !== false,
            duration: operation.duration || 0
        };
        
        // Add to history
        this.operationHistory.push(op);
        
        // Trim history if needed
        if (this.operationHistory.length > this.maxHistorySize) {
            this.operationHistory.shift();
        }
        
        // Add to transaction if in progress
        if (this.inTransaction) {
            this.transactionOperations.push(op);
        }
        
        this.emit('operation', op);
    }

    /**
     * Start a transaction
     */
    startTransaction(transactionId) {
        if (this.inTransaction) {
            throw new Error('Transaction already in progress');
        }
        
        this.touch();
        this.inTransaction = true;
        this.transactionId = transactionId || crypto.randomBytes(8).toString('hex');
        this.transactionOperations = [];
        this.transactionStartTime = new Date();
        this.state = SessionState.TRANSACTION_IN_PROGRESS;
        this.stats.transactions++;
        
        this.logger.info(`Session ${this.id} started transaction ${this.transactionId}`);
        this.emit('transactionStart', this.transactionId);
        
        return this.transactionId;
    }

    /**
     * Commit a transaction
     */
    commitTransaction() {
        if (!this.inTransaction) {
            throw new Error('No transaction in progress');
        }
        
        this.touch();
        
        const duration = Date.now() - this.transactionStartTime.getTime();
        const operationCount = this.transactionOperations.length;
        
        this.logger.info(`Session ${this.id} committed transaction ${this.transactionId} with ${operationCount} operations in ${duration}ms`);
        
        this.inTransaction = false;
        this.state = SessionState.TRANSACTION_COMMITTED;
        this.stats.commits++;
        
        const result = {
            transactionId: this.transactionId,
            operations: operationCount,
            duration
        };
        
        this.transactionId = null;
        this.transactionOperations = [];
        this.transactionStartTime = null;
        
        // Reset state after short delay
        setTimeout(() => {
            if (this.state === SessionState.TRANSACTION_COMMITTED) {
                this.state = SessionState.ACTIVE;
            }
        }, 100);
        
        this.emit('transactionCommit', result);
        return result;
    }

    /**
     * Abort a transaction
     */
    abortTransaction() {
        if (!this.inTransaction) {
            throw new Error('No transaction in progress');
        }
        
        this.touch();
        
        const duration = Date.now() - this.transactionStartTime.getTime();
        const operationCount = this.transactionOperations.length;
        
        this.logger.info(`Session ${this.id} aborted transaction ${this.transactionId} with ${operationCount} operations`);
        
        this.inTransaction = false;
        this.state = SessionState.TRANSACTION_ABORTED;
        this.stats.aborts++;
        
        const result = {
            transactionId: this.transactionId,
            operations: operationCount,
            duration
        };
        
        this.transactionId = null;
        this.transactionOperations = [];
        this.transactionStartTime = null;
        
        // Reset state after short delay
        setTimeout(() => {
            if (this.state === SessionState.TRANSACTION_ABORTED) {
                this.state = SessionState.ACTIVE;
            }
        }, 100);
        
        this.emit('transactionAbort', result);
        return result;
    }

    /**
     * Get transaction status
     */
    getTransactionStatus() {
        if (!this.inTransaction) {
            return null;
        }
        
        return {
            transactionId: this.transactionId,
            startTime: this.transactionStartTime,
            operations: this.transactionOperations.length,
            duration: Date.now() - this.transactionStartTime.getTime()
        };
    }

    /**
     * Expire the session
     */
    expire(reason = 'Manual expiration') {
        if (this.state === SessionState.EXPIRED || this.state === SessionState.ENDED) {
            return;
        }
        
        // Abort any active transaction
        if (this.inTransaction) {
            this.abortTransaction();
        }
        
        this.state = SessionState.EXPIRED;
        this.logger.info(`Session ${this.id} expired: ${reason}`);
        
        this.emit('expired', reason);
        this.cleanup();
    }

    /**
     * End the session
     */
    end() {
        if (this.state === SessionState.ENDED) {
            return;
        }
        
        // Abort any active transaction
        if (this.inTransaction) {
            this.abortTransaction();
        }
        
        this.state = SessionState.ENDED;
        this.logger.info(`Session ${this.id} ended`);
        
        this.emit('ended');
        this.cleanup();
    }

    /**
     * Clean up session resources
     */
    cleanup() {
        if (this.timeoutChecker) {
            clearInterval(this.timeoutChecker);
            this.timeoutChecker = null;
        }
        
        this.operationHistory = [];
        this.transactionOperations = [];
    }

    /**
     * Get session info
     */
    getInfo() {
        return {
            id: this.id,
            connectionId: this.connectionId,
            userId: this.userId,
            state: this.state,
            database: this.database,
            createdAt: this.createdAt,
            lastActivity: this.lastActivity,
            uptime: Date.now() - this.createdAt.getTime(),
            idleTime: Date.now() - this.lastActivity.getTime(),
            inTransaction: this.inTransaction,
            transactionId: this.transactionId,
            stats: { ...this.stats },
            recentOperations: this.operationHistory.slice(-10)
        };
    }

    /**
     * Check if session is active
     */
    isActive() {
        return this.state === SessionState.ACTIVE || 
               this.state === SessionState.IDLE ||
               this.state === SessionState.TRANSACTION_IN_PROGRESS;
    }
}

/**
 * Session Manager
 */
export class SessionManager extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.sessions = new Map();
        this.maxSessions = options.maxSessions || 10000;
        this.defaultTimeout = options.defaultTimeout || 1800000;
        this.logger = options.logger || console;
        
        // Statistics
        this.stats = {
            totalSessions: 0,
            activeSessions: 0,
            expiredSessions: 0,
            endedSessions: 0
        };
        
        // Start cleanup interval
        this.startCleanup();
    }

    /**
     * Start periodic cleanup
     */
    startCleanup() {
        this.cleanupInterval = setInterval(() => {
            this.cleanup();
        }, 300000); // Every 5 minutes
    }

    /**
     * Create a new session
     */
    createSession(options = {}) {
        if (this.sessions.size >= this.maxSessions) {
            // Try to clean up first
            this.cleanup();
            
            if (this.sessions.size >= this.maxSessions) {
                throw new Error('Maximum sessions reached');
            }
        }
        
        const session = new Session({
            ...options,
            timeout: options.timeout || this.defaultTimeout,
            logger: this.logger
        });
        
        this.sessions.set(session.id, session);
        this.stats.totalSessions++;
        this.stats.activeSessions++;
        
        // Handle session events
        session.on('expired', () => {
            this.stats.expiredSessions++;
            this.stats.activeSessions--;
            this.emit('sessionExpired', session);
        });
        
        session.on('ended', () => {
            this.stats.endedSessions++;
            this.stats.activeSessions--;
            this.emit('sessionEnded', session);
        });
        
        this.logger.info(`Created session ${session.id}`);
        this.emit('sessionCreated', session);
        
        return session;
    }

    /**
     * Get session by ID
     */
    getSession(sessionId) {
        return this.sessions.get(sessionId);
    }

    /**
     * Get all active sessions
     */
    getActiveSessions() {
        const active = [];
        for (const session of this.sessions.values()) {
            if (session.isActive()) {
                active.push(session);
            }
        }
        return active;
    }

    /**
     * Get sessions by connection ID
     */
    getSessionsByConnection(connectionId) {
        const sessions = [];
        for (const session of this.sessions.values()) {
            if (session.connectionId === connectionId) {
                sessions.push(session);
            }
        }
        return sessions;
    }

    /**
     * End a session
     */
    endSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (session) {
            session.end();
            return true;
        }
        return false;
    }

    /**
     * End all sessions for a connection
     */
    endConnectionSessions(connectionId) {
        const sessions = this.getSessionsByConnection(connectionId);
        for (const session of sessions) {
            session.end();
        }
        return sessions.length;
    }

    /**
     * Clean up expired sessions
     */
    cleanup() {
        let removed = 0;
        
        for (const [id, session] of this.sessions.entries()) {
            if (session.state === SessionState.EXPIRED || session.state === SessionState.ENDED) {
                this.sessions.delete(id);
                session.cleanup();
                removed++;
            }
        }
        
        if (removed > 0) {
            this.logger.info(`Cleaned up ${removed} sessions`);
        }
        
        return removed;
    }

    /**
     * Get manager statistics
     */
    getStats() {
        return {
            ...this.stats,
            currentSessions: this.sessions.size,
            sessions: Array.from(this.sessions.values()).map(s => s.getInfo())
        };
    }

    /**
     * Shutdown manager
     */
    shutdown() {
        // Stop cleanup interval
        if (this.cleanupInterval) {
            clearInterval(this.cleanupInterval);
            this.cleanupInterval = null;
        }
        
        // End all sessions
        for (const session of this.sessions.values()) {
            session.end();
        }
        
        this.sessions.clear();
        this.logger.info('Session manager shut down');
    }
}

// Export everything
export default Session;