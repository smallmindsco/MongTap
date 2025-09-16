/**
 * MongoDB Wire Protocol Opcodes
 * Implementation from scratch based on MongoDB wire protocol specification
 * 
 * MongoDB uses a simple request-response protocol over TCP/IP
 * Each message has a standard header followed by opcode-specific data
 */

import { BSON } from './bson.js';

/**
 * MongoDB Wire Protocol Opcodes
 * These are the operation codes used in the MongoDB wire protocol
 */
export const OpCode = {
    // Legacy opcodes (still supported for compatibility)
    OP_REPLY: 1,        // Reply to a client request (deprecated in 5.0)
    OP_UPDATE: 2001,    // Update document (deprecated in 5.0)
    OP_INSERT: 2002,    // Insert new document (deprecated in 5.0)
    OP_QUERY: 2004,     // Query a collection (deprecated in 5.0)
    OP_GET_MORE: 2005,  // Get more data from a query (deprecated in 5.0)
    OP_DELETE: 2006,    // Delete documents (deprecated in 5.0)
    OP_KILL_CURSORS: 2007, // Tell database to close cursors (deprecated in 5.0)
    
    // Modern opcodes (MongoDB 3.6+)
    OP_COMPRESSED: 2012, // Compressed message
    OP_MSG: 2013,       // Extensible message format (MongoDB 3.6+)
    
    // Internal opcodes
    OP_COMMAND: 2010,   // Internal command (deprecated)
    OP_COMMANDREPLY: 2011 // Internal command reply (deprecated)
};

/**
 * Message header structure
 * All MongoDB wire protocol messages start with this header
 */
export class MessageHeader {
    constructor() {
        this.messageLength = 0;  // Total message size including header (int32)
        this.requestID = 0;      // Identifier for this message (int32)
        this.responseTo = 0;     // RequestID from original request (int32)
        this.opCode = 0;         // Request type (int32)
    }
    
    /**
     * Size of the header in bytes
     */
    static get SIZE() {
        return 16; // 4 bytes * 4 fields
    }
    
    /**
     * Serialize header to buffer
     */
    toBuffer() {
        const buffer = Buffer.allocUnsafe(MessageHeader.SIZE);
        buffer.writeInt32LE(this.messageLength, 0);
        buffer.writeInt32LE(this.requestID, 4);
        buffer.writeInt32LE(this.responseTo, 8);
        buffer.writeInt32LE(this.opCode, 12);
        return buffer;
    }
    
    /**
     * Parse header from buffer
     */
    static fromBuffer(buffer) {
        if (buffer.length < MessageHeader.SIZE) {
            throw new Error('Buffer too small for message header');
        }
        
        const header = new MessageHeader();
        header.messageLength = buffer.readInt32LE(0);
        header.requestID = buffer.readInt32LE(4);
        header.responseTo = buffer.readInt32LE(8);
        header.opCode = buffer.readInt32LE(12);
        
        return header;
    }
}

/**
 * OP_MSG Message Format (MongoDB 3.6+)
 * This is the modern extensible message format
 */
export class OpMsgMessage {
    constructor() {
        this.header = new MessageHeader();
        this.flagBits = 0;      // Message flags (uint32)
        this.sections = [];     // Message sections
        this.checksum = null;   // Optional CRC-32C checksum
    }
    
    // Flag bit definitions
    static FLAGS = {
        CHECKSUM_PRESENT: 0x01,  // Checksum is present
        MORE_TO_COME: 0x02,      // Another message will follow
        EXHAUST_ALLOWED: 0x10000 // Client can exhaust cursor
    };
    
    /**
     * Add a document section (kind 0)
     */
    addDocument(document) {
        this.sections.push({
            kind: 0,
            document: document
        });
    }
    
    /**
     * Add a document sequence section (kind 1)
     */
    addDocumentSequence(identifier, documents) {
        this.sections.push({
            kind: 1,
            identifier: identifier,
            documents: documents
        });
    }
    
    /**
     * Calculate message size
     */
    calculateSize() {
        let size = MessageHeader.SIZE + 4; // Header + flagBits
        
        for (const section of this.sections) {
            size += 1; // Section kind byte
            
            if (section.kind === 0) {
                // Document section - size will be calculated during BSON encoding
                size += 4; // Placeholder for BSON document size
            } else if (section.kind === 1) {
                // Document sequence section
                size += 4; // Size of section
                size += Buffer.byteLength(section.identifier, 'utf8') + 1; // Identifier + null terminator
                // Documents size will be calculated during BSON encoding
            }
        }
        
        if (this.flagBits & OpMsgMessage.FLAGS.CHECKSUM_PRESENT) {
            size += 4; // CRC-32C checksum
        }
        
        return size;
    }
}

/**
 * OP_QUERY Message Format (Legacy, but still widely used)
 */
export class OpQueryMessage {
    constructor() {
        this.header = new MessageHeader();
        this.flags = 0;           // Query flags (int32)
        this.fullCollectionName = ''; // Namespace (cstring)
        this.numberToSkip = 0;    // Number of documents to skip (int32)
        this.numberToReturn = 0;  // Number of documents to return (int32)
        this.query = {};          // Query document (BSON)
        this.returnFieldsSelector = null; // Optional projection (BSON)
    }
    
    // Query flag definitions
    static FLAGS = {
        TAILABLE_CURSOR: 1 << 1,
        SLAVE_OK: 1 << 2,
        OPLOG_REPLAY: 1 << 3,
        NO_CURSOR_TIMEOUT: 1 << 4,
        AWAIT_DATA: 1 << 5,
        EXHAUST: 1 << 6,
        PARTIAL: 1 << 7
    };
}

/**
 * OP_REPLY Message Format (Response to OP_QUERY)
 */
export class OpReplyMessage {
    constructor() {
        this.header = new MessageHeader();
        this.responseFlags = 0;   // Response flags (int32)
        this.cursorID = 0n;       // Cursor identifier (int64)
        this.startingFrom = 0;    // Starting position (int32)
        this.numberReturned = 0;  // Number of documents (int32)
        this.documents = [];      // Array of BSON documents
    }
    
    // Response flag definitions
    static FLAGS = {
        CURSOR_NOT_FOUND: 1 << 0,
        QUERY_FAILURE: 1 << 1,
        SHARD_CONFIG_STALE: 1 << 2,
        AWAIT_CAPABLE: 1 << 3
    };
}

/**
 * OP_INSERT Message Format (Legacy)
 */
export class OpInsertMessage {
    constructor() {
        this.header = new MessageHeader();
        this.flags = 0;           // Insert flags (int32)
        this.fullCollectionName = ''; // Namespace (cstring)
        this.documents = [];      // Array of BSON documents to insert
    }
    
    // Insert flag definitions
    static FLAGS = {
        CONTINUE_ON_ERROR: 1 << 0
    };
}

/**
 * OP_UPDATE Message Format (Legacy)
 */
export class OpUpdateMessage {
    constructor() {
        this.header = new MessageHeader();
        this.zero = 0;            // Reserved (int32)
        this.fullCollectionName = ''; // Namespace (cstring)
        this.flags = 0;           // Update flags (int32)
        this.selector = {};       // Query to select documents (BSON)
        this.update = {};         // Update operations (BSON)
    }
    
    // Update flag definitions
    static FLAGS = {
        UPSERT: 1 << 0,
        MULTI_UPDATE: 1 << 1
    };
}

/**
 * OP_DELETE Message Format (Legacy)
 */
export class OpDeleteMessage {
    constructor() {
        this.header = new MessageHeader();
        this.zero = 0;            // Reserved (int32)
        this.fullCollectionName = ''; // Namespace (cstring)
        this.flags = 0;           // Delete flags (int32)
        this.selector = {};       // Query to select documents (BSON)
    }
    
    // Delete flag definitions
    static FLAGS = {
        SINGLE_REMOVE: 1 << 0
    };
}

/**
 * OP_GET_MORE Message Format (Legacy)
 */
export class OpGetMoreMessage {
    constructor() {
        this.header = new MessageHeader();
        this.zero = 0;            // Reserved (int32)
        this.fullCollectionName = ''; // Namespace (cstring)
        this.numberToReturn = 0;  // Number of documents to return (int32)
        this.cursorID = 0n;       // Cursor ID from OP_REPLY (int64)
    }
}

/**
 * OP_KILL_CURSORS Message Format
 */
export class OpKillCursorsMessage {
    constructor() {
        this.header = new MessageHeader();
        this.zero = 0;            // Reserved (int32)
        this.numberOfCursorIDs = 0; // Number of cursor IDs (int32)
        this.cursorIDs = [];      // Array of cursor IDs to close (int64[])
    }
}

/**
 * Helper class for parsing opcodes
 */
export class OpcodeParser {
    /**
     * Parse a message based on its opcode
     */
    static parseMessage(buffer) {
        if (buffer.length < MessageHeader.SIZE) {
            throw new Error('Buffer too small for message');
        }
        
        const header = MessageHeader.fromBuffer(buffer);
        
        // Validate message length
        if (header.messageLength > buffer.length) {
            throw new Error(`Incomplete message: expected ${header.messageLength} bytes, got ${buffer.length}`);
        }
        
        // Parse based on opcode
        switch (header.opCode) {
            case OpCode.OP_MSG:
                return this.parseOpMsg(header, buffer.slice(MessageHeader.SIZE));
                
            case OpCode.OP_QUERY:
                return this.parseOpQuery(header, buffer.slice(MessageHeader.SIZE));
                
            case OpCode.OP_REPLY:
                return this.parseOpReply(header, buffer.slice(MessageHeader.SIZE));
                
            case OpCode.OP_INSERT:
                return this.parseOpInsert(header, buffer.slice(MessageHeader.SIZE));
                
            case OpCode.OP_UPDATE:
                return this.parseOpUpdate(header, buffer.slice(MessageHeader.SIZE));
                
            case OpCode.OP_DELETE:
                return this.parseOpDelete(header, buffer.slice(MessageHeader.SIZE));
                
            case OpCode.OP_GET_MORE:
                return this.parseOpGetMore(header, buffer.slice(MessageHeader.SIZE));
                
            case OpCode.OP_KILL_CURSORS:
                return this.parseOpKillCursors(header, buffer.slice(MessageHeader.SIZE));
                
            default:
                throw new Error(`Unknown opcode: ${header.opCode}`);
        }
    }
    
    /**
     * Parse OP_MSG message body
     */
    static parseOpMsg(header, buffer) {
        const msg = new OpMsgMessage();
        msg.header = header;
        
        // Parse flag bits
        msg.flagBits = buffer.readUInt32LE(0);
        let offset = 4;
        
        // Parse sections
        while (offset < buffer.length - (msg.flagBits & OpMsgMessage.FLAGS.CHECKSUM_PRESENT ? 4 : 0)) {
            const kind = buffer[offset++];
            
            if (kind === 0) {
                // Document section - parse BSON document
                const docSize = buffer.readInt32LE(offset);
                const docBuffer = buffer.slice(offset, offset + docSize);
                const document = BSON.deserialize(docBuffer);
                
                msg.sections.push({
                    kind: 0,
                    document: document,
                    rawData: docBuffer
                });
                offset += docSize;
            } else if (kind === 1) {
                // Document sequence section
                const sectionSize = buffer.readInt32LE(offset);
                offset += 4;
                const sectionEnd = offset + sectionSize - 4;
                
                // Read identifier (C string)
                const identifierEnd = buffer.indexOf(0, offset);
                const identifier = buffer.toString('utf8', offset, identifierEnd);
                offset = identifierEnd + 1;
                
                // Parse documents in this section
                const documents = [];
                while (offset < sectionEnd) {
                    const docSize = buffer.readInt32LE(offset);
                    const docBuffer = buffer.slice(offset, offset + docSize);
                    const document = BSON.deserialize(docBuffer);
                    documents.push(document);
                    offset += docSize;
                }
                
                msg.sections.push({
                    kind: 1,
                    identifier: identifier,
                    documents: documents
                });
            }
        }
        
        // Parse checksum if present
        if (msg.flagBits & OpMsgMessage.FLAGS.CHECKSUM_PRESENT) {
            msg.checksum = buffer.readUInt32LE(buffer.length - 4);
        }
        
        return msg;
    }
    
    /**
     * Parse OP_QUERY message body
     */
    static parseOpQuery(header, buffer) {
        const msg = new OpQueryMessage();
        msg.header = header;
        
        let offset = 0;
        
        // Parse flags
        msg.flags = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse collection name (C string)
        const nameEnd = buffer.indexOf(0, offset);
        msg.fullCollectionName = buffer.toString('utf8', offset, nameEnd);
        offset = nameEnd + 1;
        
        // Parse skip and limit
        msg.numberToSkip = buffer.readInt32LE(offset);
        offset += 4;
        msg.numberToReturn = buffer.readInt32LE(offset);
        offset += 4;
        
        // Remaining buffer contains BSON documents (query and optional projection)
        msg.queryBuffer = buffer.slice(offset);
        
        return msg;
    }
    
    /**
     * Parse OP_REPLY message body
     */
    static parseOpReply(header, buffer) {
        const msg = new OpReplyMessage();
        msg.header = header;
        
        let offset = 0;
        
        // Parse response flags
        msg.responseFlags = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse cursor ID (64-bit)
        msg.cursorID = buffer.readBigInt64LE(offset);
        offset += 8;
        
        // Parse starting position
        msg.startingFrom = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse number returned
        msg.numberReturned = buffer.readInt32LE(offset);
        offset += 4;
        
        // Remaining buffer contains BSON documents
        msg.documentsBuffer = buffer.slice(offset);
        
        return msg;
    }
    
    /**
     * Parse OP_INSERT message body (simplified)
     */
    static parseOpInsert(header, buffer) {
        const msg = new OpInsertMessage();
        msg.header = header;
        
        let offset = 0;
        
        // Parse flags
        msg.flags = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse collection name
        const nameEnd = buffer.indexOf(0, offset);
        msg.fullCollectionName = buffer.toString('utf8', offset, nameEnd);
        offset = nameEnd + 1;
        
        // Remaining buffer contains BSON documents
        msg.documentsBuffer = buffer.slice(offset);
        
        return msg;
    }
    
    /**
     * Parse OP_UPDATE message body (simplified)
     */
    static parseOpUpdate(header, buffer) {
        const msg = new OpUpdateMessage();
        msg.header = header;
        
        let offset = 0;
        
        // Parse zero field
        msg.zero = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse collection name
        const nameEnd = buffer.indexOf(0, offset);
        msg.fullCollectionName = buffer.toString('utf8', offset, nameEnd);
        offset = nameEnd + 1;
        
        // Parse flags
        msg.flags = buffer.readInt32LE(offset);
        offset += 4;
        
        // Remaining buffer contains BSON documents (selector and update)
        msg.documentsBuffer = buffer.slice(offset);
        
        return msg;
    }
    
    /**
     * Parse OP_DELETE message body (simplified)
     */
    static parseOpDelete(header, buffer) {
        const msg = new OpDeleteMessage();
        msg.header = header;
        
        let offset = 0;
        
        // Parse zero field
        msg.zero = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse collection name
        const nameEnd = buffer.indexOf(0, offset);
        msg.fullCollectionName = buffer.toString('utf8', offset, nameEnd);
        offset = nameEnd + 1;
        
        // Parse flags
        msg.flags = buffer.readInt32LE(offset);
        offset += 4;
        
        // Remaining buffer contains BSON selector
        msg.selectorBuffer = buffer.slice(offset);
        
        return msg;
    }
    
    /**
     * Parse OP_GET_MORE message body
     */
    static parseOpGetMore(header, buffer) {
        const msg = new OpGetMoreMessage();
        msg.header = header;
        
        let offset = 0;
        
        // Parse zero field
        msg.zero = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse collection name
        const nameEnd = buffer.indexOf(0, offset);
        msg.fullCollectionName = buffer.toString('utf8', offset, nameEnd);
        offset = nameEnd + 1;
        
        // Parse number to return
        msg.numberToReturn = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse cursor ID
        msg.cursorID = buffer.readBigInt64LE(offset);
        
        return msg;
    }
    
    /**
     * Parse OP_KILL_CURSORS message body
     */
    static parseOpKillCursors(header, buffer) {
        const msg = new OpKillCursorsMessage();
        msg.header = header;
        
        let offset = 0;
        
        // Parse zero field
        msg.zero = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse number of cursor IDs
        msg.numberOfCursorIDs = buffer.readInt32LE(offset);
        offset += 4;
        
        // Parse cursor IDs
        msg.cursorIDs = [];
        for (let i = 0; i < msg.numberOfCursorIDs; i++) {
            msg.cursorIDs.push(buffer.readBigInt64LE(offset));
            offset += 8;
        }
        
        return msg;
    }
}

/**
 * Get opcode name from value
 */
export function getOpcodeName(opcode) {
    for (const [name, value] of Object.entries(OpCode)) {
        if (value === opcode) {
            return name;
        }
    }
    return `UNKNOWN(${opcode})`;
}