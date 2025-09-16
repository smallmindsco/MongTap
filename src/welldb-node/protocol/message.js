/**
 * MongoDB Wire Protocol Message Implementation
 * Handles message framing and protocol-level communication
 */

import { BSON } from './bson.js';
import {
    OpCode,
    MessageHeader,
    OpMsgMessage,
    OpQueryMessage,
    OpReplyMessage,
    OpInsertMessage,
    OpUpdateMessage,
    OpDeleteMessage,
    OpGetMoreMessage,
    OpKillCursorsMessage,
    OpcodeParser
} from './opcodes.js';

// Message flags for OP_MSG
export const MessageFlags = {
    CHECKSUM_PRESENT: 0x00000001,
    MORE_TO_COME: 0x00000002,
    EXHAUST_ALLOWED: 0x00010000
};

/**
 * MongoDB Wire Protocol Message Handler
 * Manages message creation, parsing, and response generation
 */
export class MessageHandler {
    constructor(logger = null) {
        this.logger = logger || this.createDefaultLogger();
        this.requestIdCounter = 1;
    }

    createDefaultLogger() {
        return {
            debug: () => {},
            info: () => {},
            warn: console.warn,
            error: console.error
        };
    }

    /**
     * Generate next request ID
     */
    getNextRequestId() {
        return this.requestIdCounter++;
    }

    /**
     * Create an OP_MSG message
     */
    createOpMsg(document, flags = 0, responseTo = 0) {
        const msg = new OpMsgMessage();
        msg.header.opCode = OpCode.OP_MSG;
        msg.header.requestID = this.getNextRequestId();
        msg.header.responseTo = responseTo;
        msg.flagBits = flags;
        msg.addDocument(document);
        
        return this.serializeMessage(msg);
    }

    /**
     * Create an OP_QUERY message
     */
    createOpQuery(collection, query, projection = null, skip = 0, limit = 0, flags = 0) {
        const msg = new OpQueryMessage();
        msg.header.opCode = OpCode.OP_QUERY;
        msg.header.requestID = this.getNextRequestId();
        msg.header.responseTo = 0;
        msg.flags = flags;
        msg.fullCollectionName = collection;
        msg.numberToSkip = skip;
        msg.numberToReturn = limit;
        msg.query = query;
        msg.returnFieldsSelector = projection;
        
        return this.serializeMessage(msg);
    }

    /**
     * Create an OP_REPLY message
     */
    createOpReply(responseTo, documents = [], cursorId = 0n, flags = 0) {
        const msg = new OpReplyMessage();
        msg.header.opCode = OpCode.OP_REPLY;
        msg.header.requestID = this.getNextRequestId();
        msg.header.responseTo = responseTo;
        msg.responseFlags = flags;
        msg.cursorID = cursorId;
        msg.startingFrom = 0;
        msg.numberReturned = documents.length;
        msg.documents = documents;
        
        return this.serializeMessage(msg);
    }

    /**
     * Create an OP_INSERT message
     */
    createOpInsert(collection, documents, flags = 0) {
        const msg = new OpInsertMessage();
        msg.header.opCode = OpCode.OP_INSERT;
        msg.header.requestID = this.getNextRequestId();
        msg.header.responseTo = 0;
        msg.flags = flags;
        msg.fullCollectionName = collection;
        msg.documents = Array.isArray(documents) ? documents : [documents];
        
        return this.serializeMessage(msg);
    }

    /**
     * Create an OP_UPDATE message
     */
    createOpUpdate(collection, selector, update, flags = 0) {
        const msg = new OpUpdateMessage();
        msg.header.opCode = OpCode.OP_UPDATE;
        msg.header.requestID = this.getNextRequestId();
        msg.header.responseTo = 0;
        msg.ZERO = 0;
        msg.fullCollectionName = collection;
        msg.flags = flags;
        msg.selector = selector;
        msg.update = update;
        
        return this.serializeMessage(msg);
    }

    /**
     * Create an OP_DELETE message
     */
    createOpDelete(collection, selector, flags = 0) {
        const msg = new OpDeleteMessage();
        msg.header.opCode = OpCode.OP_DELETE;
        msg.header.requestID = this.getNextRequestId();
        msg.header.responseTo = 0;
        msg.ZERO = 0;
        msg.fullCollectionName = collection;
        msg.flags = flags;
        msg.selector = selector;
        
        return this.serializeMessage(msg);
    }

    /**
     * Create an OP_GET_MORE message
     */
    createOpGetMore(collection, cursorId, numberToReturn = 0) {
        const msg = new OpGetMoreMessage();
        msg.header.opCode = OpCode.OP_GET_MORE;
        msg.header.requestID = this.getNextRequestId();
        msg.header.responseTo = 0;
        msg.ZERO = 0;
        msg.fullCollectionName = collection;
        msg.numberToReturn = numberToReturn;
        msg.cursorID = cursorId;
        
        return this.serializeMessage(msg);
    }

    /**
     * Create an OP_KILL_CURSORS message
     */
    createOpKillCursors(cursorIds) {
        const msg = new OpKillCursorsMessage();
        msg.header.opCode = OpCode.OP_KILL_CURSORS;
        msg.header.requestID = this.getNextRequestId();
        msg.header.responseTo = 0;
        msg.ZERO = 0;
        msg.numberOfCursorIDs = cursorIds.length;
        msg.cursorIDs = cursorIds;
        
        return this.serializeMessage(msg);
    }

    /**
     * Serialize a message to buffer
     */
    serializeMessage(message) {
        const buffers = [];
        
        // Serialize based on message type
        switch (message.header.opCode) {
            case OpCode.OP_MSG:
                return this.serializeOpMsg(message);
            case OpCode.OP_QUERY:
                return this.serializeOpQuery(message);
            case OpCode.OP_REPLY:
                return this.serializeOpReply(message);
            case OpCode.OP_INSERT:
                return this.serializeOpInsert(message);
            case OpCode.OP_UPDATE:
                return this.serializeOpUpdate(message);
            case OpCode.OP_DELETE:
                return this.serializeOpDelete(message);
            case OpCode.OP_GET_MORE:
                return this.serializeOpGetMore(message);
            case OpCode.OP_KILL_CURSORS:
                return this.serializeOpKillCursors(message);
            default:
                throw new Error(`Unsupported opcode: ${message.header.opCode}`);
        }
    }

    /**
     * Serialize OP_MSG
     */
    serializeOpMsg(msg) {
        const bodyBuffers = [];
        
        // Add flag bits
        const flagBuffer = Buffer.alloc(4);
        flagBuffer.writeUInt32LE(msg.flagBits, 0);
        bodyBuffers.push(flagBuffer);
        
        // Add sections
        for (const section of msg.sections) {
            bodyBuffers.push(Buffer.from([section.kind]));
            
            if (section.kind === 0) {
                // Document section
                const docBuffer = BSON.serialize(section.document);
                bodyBuffers.push(docBuffer);
            } else if (section.kind === 1) {
                // Document sequence section
                const seqBuffers = [];
                
                // Add identifier
                seqBuffers.push(Buffer.from(section.identifier, 'utf8'));
                seqBuffers.push(Buffer.from([0])); // null terminator
                
                // Add documents
                for (const doc of section.documents) {
                    seqBuffers.push(BSON.serialize(doc));
                }
                
                // Calculate section size
                const seqContent = Buffer.concat(seqBuffers);
                const sizeBuffer = Buffer.alloc(4);
                sizeBuffer.writeInt32LE(seqContent.length + 4, 0);
                
                bodyBuffers.push(sizeBuffer);
                bodyBuffers.push(seqContent);
            }
        }
        
        // Add checksum if flag is set
        if (msg.flagBits & MessageFlags.CHECKSUM_PRESENT) {
            // TODO: Implement CRC32C checksum
            bodyBuffers.push(Buffer.alloc(4)); // Placeholder
        }
        
        const body = Buffer.concat(bodyBuffers);
        
        // Update header with total size
        msg.header.messageLength = 16 + body.length; // Header + body
        
        return Buffer.concat([msg.header.toBuffer(), body]);
    }

    /**
     * Serialize OP_QUERY
     */
    serializeOpQuery(msg) {
        const bodyBuffers = [];
        
        // Flags
        const flagBuffer = Buffer.alloc(4);
        flagBuffer.writeInt32LE(msg.flags, 0);
        bodyBuffers.push(flagBuffer);
        
        // Collection name
        bodyBuffers.push(Buffer.from(msg.fullCollectionName, 'utf8'));
        bodyBuffers.push(Buffer.from([0])); // null terminator
        
        // Skip and return
        const skipBuffer = Buffer.alloc(4);
        skipBuffer.writeInt32LE(msg.numberToSkip, 0);
        bodyBuffers.push(skipBuffer);
        
        const returnBuffer = Buffer.alloc(4);
        returnBuffer.writeInt32LE(msg.numberToReturn, 0);
        bodyBuffers.push(returnBuffer);
        
        // Query document
        bodyBuffers.push(BSON.serialize(msg.query));
        
        // Optional projection
        if (msg.returnFieldsSelector) {
            bodyBuffers.push(BSON.serialize(msg.returnFieldsSelector));
        }
        
        const body = Buffer.concat(bodyBuffers);
        msg.header.messageLength = 16 + body.length;
        
        return Buffer.concat([msg.header.toBuffer(), body]);
    }

    /**
     * Serialize OP_REPLY
     */
    serializeOpReply(msg) {
        const bodyBuffers = [];
        
        // Response flags
        const flagBuffer = Buffer.alloc(4);
        flagBuffer.writeInt32LE(msg.responseFlags, 0);
        bodyBuffers.push(flagBuffer);
        
        // Cursor ID
        const cursorBuffer = Buffer.alloc(8);
        cursorBuffer.writeBigInt64LE(msg.cursorID, 0);
        bodyBuffers.push(cursorBuffer);
        
        // Starting from
        const startBuffer = Buffer.alloc(4);
        startBuffer.writeInt32LE(msg.startingFrom, 0);
        bodyBuffers.push(startBuffer);
        
        // Number returned
        const numBuffer = Buffer.alloc(4);
        numBuffer.writeInt32LE(msg.numberReturned, 0);
        bodyBuffers.push(numBuffer);
        
        // Documents
        for (const doc of msg.documents) {
            bodyBuffers.push(BSON.serialize(doc));
        }
        
        const body = Buffer.concat(bodyBuffers);
        msg.header.messageLength = 16 + body.length;
        
        return Buffer.concat([msg.header.toBuffer(), body]);
    }

    /**
     * Serialize OP_INSERT
     */
    serializeOpInsert(msg) {
        const bodyBuffers = [];
        
        // Flags
        const flagBuffer = Buffer.alloc(4);
        flagBuffer.writeInt32LE(msg.flags, 0);
        bodyBuffers.push(flagBuffer);
        
        // Collection name
        bodyBuffers.push(Buffer.from(msg.fullCollectionName, 'utf8'));
        bodyBuffers.push(Buffer.from([0])); // null terminator
        
        // Documents
        for (const doc of msg.documents) {
            bodyBuffers.push(BSON.serialize(doc));
        }
        
        const body = Buffer.concat(bodyBuffers);
        msg.header.messageLength = 16 + body.length;
        
        return Buffer.concat([msg.header.toBuffer(), body]);
    }

    /**
     * Serialize OP_UPDATE
     */
    serializeOpUpdate(msg) {
        const bodyBuffers = [];
        
        // ZERO
        bodyBuffers.push(Buffer.alloc(4));
        
        // Collection name
        bodyBuffers.push(Buffer.from(msg.fullCollectionName, 'utf8'));
        bodyBuffers.push(Buffer.from([0])); // null terminator
        
        // Flags
        const flagBuffer = Buffer.alloc(4);
        flagBuffer.writeInt32LE(msg.flags, 0);
        bodyBuffers.push(flagBuffer);
        
        // Selector and update documents
        bodyBuffers.push(BSON.serialize(msg.selector));
        bodyBuffers.push(BSON.serialize(msg.update));
        
        const body = Buffer.concat(bodyBuffers);
        msg.header.messageLength = 16 + body.length;
        
        return Buffer.concat([msg.header.toBuffer(), body]);
    }

    /**
     * Serialize OP_DELETE
     */
    serializeOpDelete(msg) {
        const bodyBuffers = [];
        
        // ZERO
        bodyBuffers.push(Buffer.alloc(4));
        
        // Collection name
        bodyBuffers.push(Buffer.from(msg.fullCollectionName, 'utf8'));
        bodyBuffers.push(Buffer.from([0])); // null terminator
        
        // Flags
        const flagBuffer = Buffer.alloc(4);
        flagBuffer.writeInt32LE(msg.flags, 0);
        bodyBuffers.push(flagBuffer);
        
        // Selector document
        bodyBuffers.push(BSON.serialize(msg.selector));
        
        const body = Buffer.concat(bodyBuffers);
        msg.header.messageLength = 16 + body.length;
        
        return Buffer.concat([msg.header.toBuffer(), body]);
    }

    /**
     * Serialize OP_GET_MORE
     */
    serializeOpGetMore(msg) {
        const bodyBuffers = [];
        
        // ZERO
        bodyBuffers.push(Buffer.alloc(4));
        
        // Collection name
        bodyBuffers.push(Buffer.from(msg.fullCollectionName, 'utf8'));
        bodyBuffers.push(Buffer.from([0])); // null terminator
        
        // Number to return
        const numBuffer = Buffer.alloc(4);
        numBuffer.writeInt32LE(msg.numberToReturn, 0);
        bodyBuffers.push(numBuffer);
        
        // Cursor ID
        const cursorBuffer = Buffer.alloc(8);
        cursorBuffer.writeBigInt64LE(msg.cursorID, 0);
        bodyBuffers.push(cursorBuffer);
        
        const body = Buffer.concat(bodyBuffers);
        msg.header.messageLength = 16 + body.length;
        
        return Buffer.concat([msg.header.toBuffer(), body]);
    }

    /**
     * Serialize OP_KILL_CURSORS
     */
    serializeOpKillCursors(msg) {
        const bodyBuffers = [];
        
        // ZERO
        bodyBuffers.push(Buffer.alloc(4));
        
        // Number of cursor IDs
        const numBuffer = Buffer.alloc(4);
        numBuffer.writeInt32LE(msg.numberOfCursorIDs, 0);
        bodyBuffers.push(numBuffer);
        
        // Cursor IDs
        for (const cursorId of msg.cursorIDs) {
            const cursorBuffer = Buffer.alloc(8);
            cursorBuffer.writeBigInt64LE(cursorId, 0);
            bodyBuffers.push(cursorBuffer);
        }
        
        const body = Buffer.concat(bodyBuffers);
        msg.header.messageLength = 16 + body.length;
        
        return Buffer.concat([msg.header.toBuffer(), body]);
    }

    /**
     * Parse incoming message buffer
     */
    parseMessage(buffer) {
        try {
            const message = OpcodeParser.parseMessage(buffer);
            this.logger.debug(`Parsed message: ${message.constructor.name}`);
            return message;
        } catch (err) {
            this.logger.error(`Failed to parse message: ${err.message}`);
            throw err;
        }
    }

    /**
     * Create error reply
     */
    createErrorReply(responseTo, errorMessage, errorCode = 1) {
        const errorDoc = {
            ok: 0,
            errmsg: errorMessage,
            code: errorCode
        };
        
        return this.createOpReply(responseTo, [errorDoc], 0n, OpReplyMessage.FLAGS.QUERY_FAILURE);
    }

    /**
     * Create success reply
     */
    createSuccessReply(responseTo, result = {}) {
        const successDoc = {
            ok: 1,
            ...result
        };
        
        return this.createOpReply(responseTo, [successDoc]);
    }
}

// Export everything
export default MessageHandler;