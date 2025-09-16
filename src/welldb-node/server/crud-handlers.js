/**
 * CRUD Handlers with DataFlood Integration
 * 
 * Handles MongoDB CRUD operations by integrating:
 * - Collection Manager for document storage and model training
 * - Query Engine for constraint-based generation
 * - DataFlood Storage for model persistence
 */

import { CollectionManager } from '../core/collection-manager.js';
import { QueryEngine } from '../core/query-engine.js';
import { BSONSerializer } from '../protocol/bson.js';

/**
 * CRUD handler implementation
 */
export class CRUDHandlers {
    constructor(options = {}) {
        this.storage = options.storage;
        this.messageHandler = options.messageHandler;
        this.logger = options.logger || this.createDefaultLogger();
        
        // Collection manager for managing collections
        this.collectionManager = new CollectionManager({ 
            storage: this.storage,
            logger: this.logger
        });
        
        // Query engine for executing queries
        this.queryEngine = new QueryEngine({
            logger: this.logger
        });
        
        // Statistics
        this.stats = {
            inserts: 0,
            queries: 0,
            updates: 0,
            deletes: 0,
            errors: 0
        };
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
     * Handle INSERT operation
     */
    async handleInsert(fullCollectionName, documents, flags = 0) {
        const startTime = Date.now();
        this.stats.inserts++;
        
        try {
            // Parse database and collection
            const [database, collectionName] = fullCollectionName.split('.');
            
            if (!database || !collectionName) {
                throw new Error(`Invalid collection name: ${fullCollectionName}`);
            }
            
            // Get or create collection
            const collection = await this.collectionManager.getCollection(database, collectionName);
            
            // Insert documents (this will train the model)
            const result = await collection.insert(documents);
            
            const duration = Date.now() - startTime;
            this.logger.info(
                `Inserted ${result.insertedCount} documents into ${fullCollectionName} in ${duration}ms`
            );
            
            return {
                ok: 1,
                n: result.insertedCount,
                insertedIds: result.insertedIds
            };
            
        } catch (error) {
            this.stats.errors++;
            this.logger.error(`Insert error on ${fullCollectionName}:`, error);
            throw error;
        }
    }
    
    /**
     * Handle FIND/QUERY operation
     */
    async handleFind(fullCollectionName, query = {}, options = {}) {
        const startTime = Date.now();
        this.stats.queries++;
        
        try {
            // Parse database and collection
            const [database, collectionName] = fullCollectionName.split('.');
            
            if (!database || !collectionName) {
                throw new Error(`Invalid collection name: ${fullCollectionName}`);
            }
            
            // Get or create collection
            const collection = await this.collectionManager.getCollection(database, collectionName);
            
            // Execute query using collection's find method
            const documents = await collection.find(
                query,
                {
                    skip: options.skip || 0,
                    limit: options.limit || 100,
                    sort: options.sort,
                    projection: options.projection
                }
            );
            
            const duration = Date.now() - startTime;
            this.logger.info(
                `Found ${documents.length} documents in ${fullCollectionName} in ${duration}ms`
            );
            
            return {
                documents,
                cursorId: 0n, // No cursor support yet
                startingFrom: options.skip || 0
            };
            
        } catch (error) {
            this.stats.errors++;
            this.logger.error(`Find error on ${fullCollectionName}:`, error);
            throw error;
        }
    }
    
    /**
     * Handle UPDATE operation
     */
    async handleUpdate(fullCollectionName, selector, update, flags = 0) {
        const startTime = Date.now();
        this.stats.updates++;
        
        try {
            // Parse database and collection
            const [database, collectionName] = fullCollectionName.split('.');
            
            if (!database || !collectionName) {
                throw new Error(`Invalid collection name: ${fullCollectionName}`);
            }
            
            // Get or create collection
            const collection = await this.collectionManager.getCollection(database, collectionName);
            
            // Determine update options from flags
            const options = {
                multi: !!(flags & 0x02),
                upsert: !!(flags & 0x01)
            };
            
            // Perform update
            const result = await collection.update(selector, update, options);
            
            const duration = Date.now() - startTime;
            this.logger.info(
                `Updated ${result.modifiedCount} documents in ${fullCollectionName} in ${duration}ms`
            );
            
            return {
                ok: 1,
                n: result.matchedCount,
                nModified: result.modifiedCount,
                upserted: result.upsertedCount > 0 ? result.upsertedId : undefined
            };
            
        } catch (error) {
            this.stats.errors++;
            this.logger.error(`Update error on ${fullCollectionName}:`, error);
            throw error;
        }
    }
    
    /**
     * Handle DELETE operation
     */
    async handleDelete(fullCollectionName, selector, flags = 0) {
        const startTime = Date.now();
        this.stats.deletes++;
        
        try {
            // Parse database and collection
            const [database, collectionName] = fullCollectionName.split('.');
            
            if (!database || !collectionName) {
                throw new Error(`Invalid collection name: ${fullCollectionName}`);
            }
            
            // Get or create collection
            const collection = await this.collectionManager.getCollection(database, collectionName);
            
            // Determine delete options from flags
            const options = {
                single: !(flags & 0x01) // If SingleRemove flag is NOT set, delete all
            };
            
            // Perform delete
            const result = await collection.delete(selector, options);
            
            const duration = Date.now() - startTime;
            this.logger.info(
                `Deleted ${result.deletedCount} documents from ${fullCollectionName} in ${duration}ms`
            );
            
            return {
                ok: 1,
                n: result.deletedCount
            };
            
        } catch (error) {
            this.stats.errors++;
            this.logger.error(`Delete error on ${fullCollectionName}:`, error);
            throw error;
        }
    }
    
    /**
     * Handle modern INSERT command (OP_MSG)
     */
    async handleInsertCommand(command) {
        const collection = `${command.$db || command.db || 'test'}.${command.insert}`;
        const documents = command.documents || [];
        const options = {
            ordered: command.ordered !== false,
            writeConcern: command.writeConcern
        };
        
        try {
            const result = await this.handleInsert(collection, documents, 0);
            
            return {
                ok: 1,
                n: result.n,
                insertedIds: result.insertedIds
            };
            
        } catch (error) {
            return {
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            };
        }
    }
    
    /**
     * Handle modern FIND command (OP_MSG)
     */
    async handleFindCommand(command) {
        const collection = `${command.$db || command.db || 'test'}.${command.find}`;
        const filter = command.filter || {};
        const options = {
            skip: command.skip || 0,
            limit: command.limit || 100,
            sort: command.sort,
            projection: command.projection
        };
        
        try {
            const result = await this.handleFind(collection, filter, options);
            
            return {
                cursor: {
                    firstBatch: result.documents,
                    id: result.cursorId,
                    ns: collection
                },
                ok: 1
            };
            
        } catch (error) {
            return {
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            };
        }
    }
    
    /**
     * Handle modern UPDATE command (OP_MSG)
     */
    async handleUpdateCommand(command) {
        const collection = `${command.$db || command.db || 'test'}.${command.update}`;
        const updates = command.updates || [];
        
        let totalMatched = 0;
        let totalModified = 0;
        let totalUpserted = 0;
        const upsertedIds = [];
        
        try {
            for (const update of updates) {
                const result = await this.handleUpdate(
                    collection,
                    update.q || {},
                    update.u || {},
                    (update.multi ? 0x02 : 0) | (update.upsert ? 0x01 : 0)
                );
                
                totalMatched += result.n || 0;
                totalModified += result.nModified || 0;
                
                if (result.upserted) {
                    totalUpserted++;
                    upsertedIds.push({ index: updates.indexOf(update), _id: result.upserted });
                }
            }
            
            return {
                ok: 1,
                n: totalMatched,
                nModified: totalModified,
                upserted: upsertedIds.length > 0 ? upsertedIds : undefined
            };
            
        } catch (error) {
            return {
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            };
        }
    }
    
    /**
     * Handle modern DELETE command (OP_MSG)
     */
    async handleDeleteCommand(command) {
        const collection = `${command.$db || command.db || 'test'}.${command.delete}`;
        const deletes = command.deletes || [];
        
        let totalDeleted = 0;
        
        try {
            for (const del of deletes) {
                const result = await this.handleDelete(
                    collection,
                    del.q || {},
                    del.limit === 1 ? 0 : 0x01
                );
                
                totalDeleted += result.n || 0;
            }
            
            return {
                ok: 1,
                n: totalDeleted
            };
            
        } catch (error) {
            return {
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            };
        }
    }
    
    /**
     * Handle AGGREGATION command
     */
    async handleAggregateCommand(command) {
        const collection = `${command.$db || command.db || 'test'}.${command.aggregate}`;
        const pipeline = command.pipeline || [];
        const options = {
            cursor: command.cursor,
            explain: command.explain,
            allowDiskUse: command.allowDiskUse
        };
        
        try {
            // Parse database and collection
            const [database, collectionName] = collection.split('.');
            
            // Get or create collection
            const col = await this.collectionManager.getCollection(database, collectionName);
            
            // Execute aggregation
            const documents = await this.queryEngine.executeAggregation(
                col,
                pipeline,
                options
            );
            
            return {
                cursor: {
                    firstBatch: documents,
                    id: 0n,
                    ns: collection
                },
                ok: 1
            };
            
        } catch (error) {
            return {
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            };
        }
    }
    
    /**
     * Handle COUNT command
     */
    async handleCountCommand(command) {
        const collection = `${command.$db || command.db || 'test'}.${command.count}`;
        const query = command.query || {};
        const options = {
            skip: command.skip,
            limit: command.limit
        };
        
        try {
            // Parse database and collection
            const [database, collectionName] = collection.split('.');
            
            // Get or create collection
            const col = await this.collectionManager.getCollection(database, collectionName);
            
            // Count documents
            const count = await col.count(query);
            
            return {
                ok: 1,
                n: count
            };
            
        } catch (error) {
            return {
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            };
        }
    }
    
    /**
     * Handle CREATE INDEX command
     */
    async handleCreateIndexCommand(command) {
        const collection = `${command.$db || command.db || 'test'}.${command.createIndexes}`;
        const indexes = command.indexes || [];
        
        try {
            // Parse database and collection
            const [database, collectionName] = collection.split('.');
            
            // Get or create collection
            const col = await this.collectionManager.getCollection(database, collectionName);
            
            const indexNames = [];
            for (const index of indexes) {
                const name = col.createIndex(index.key, {
                    name: index.name,
                    unique: index.unique,
                    sparse: index.sparse
                });
                indexNames.push(name);
            }
            
            return {
                ok: 1,
                createdCollectionAutomatically: false,
                numIndexesBefore: col.indexes.size - indexes.length,
                numIndexesAfter: col.indexes.size,
                indexNames
            };
            
        } catch (error) {
            return {
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            };
        }
    }
    
    /**
     * Handle LIST INDEXES command
     */
    async handleListIndexesCommand(command) {
        const collection = `${command.$db || command.db || 'test'}.${command.listIndexes}`;
        
        try {
            // Parse database and collection
            const [database, collectionName] = collection.split('.');
            
            // Get collection if it exists
            const col = await this.collectionManager.getCollection(database, collectionName);
            
            const indexes = Array.from(col.indexes.values());
            
            return {
                cursor: {
                    firstBatch: indexes,
                    id: 0n,
                    ns: collection
                },
                ok: 1
            };
            
        } catch (error) {
            return {
                ok: 0,
                errmsg: error.message,
                code: error.code || 1
            };
        }
    }
    
    /**
     * Get statistics
     */
    getStats() {
        return {
            ...this.stats,
            collections: this.collectionManager.stats,
            query: this.queryEngine.getStats()
        };
    }
    
    /**
     * Clear all data
     */
    clear() {
        this.collectionManager.clear();
        this.queryEngine.clearCache();
        this.stats = {
            inserts: 0,
            queries: 0,
            updates: 0,
            deletes: 0,
            errors: 0
        };
    }
}

export default CRUDHandlers;