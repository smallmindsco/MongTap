/**
 * Minimal BSON Serialization/Deserialization
 * Implements core BSON types needed for MongoDB protocol
 * Following BSON spec: http://bsonspec.org/
 */

// BSON Type codes
export const BSONType = {
    DOUBLE: 0x01,
    STRING: 0x02,
    DOCUMENT: 0x03,
    ARRAY: 0x04,
    BINARY: 0x05,
    UNDEFINED: 0x06,  // Deprecated
    OBJECTID: 0x07,
    BOOLEAN: 0x08,
    DATE: 0x09,
    NULL: 0x0A,
    REGEX: 0x0B,
    DBPOINTER: 0x0C,  // Deprecated
    CODE: 0x0D,
    SYMBOL: 0x0E,     // Deprecated
    CODE_W_SCOPE: 0x0F,
    INT32: 0x10,
    TIMESTAMP: 0x11,
    INT64: 0x12,
    DECIMAL128: 0x13,
    MIN_KEY: 0xFF,
    MAX_KEY: 0x7F
};

// ObjectId class
export class ObjectId {
    constructor(id) {
        if (!id) {
            // Generate new ObjectId
            this.id = Buffer.alloc(12);
            // 4-byte timestamp
            this.id.writeUInt32BE(Math.floor(Date.now() / 1000), 0);
            // 5-byte random value
            this.id[4] = Math.floor(Math.random() * 256);
            this.id[5] = Math.floor(Math.random() * 256);
            this.id[6] = Math.floor(Math.random() * 256);
            this.id[7] = Math.floor(Math.random() * 256);
            this.id[8] = Math.floor(Math.random() * 256);
            // 3-byte incrementing counter
            if (!ObjectId.counter) ObjectId.counter = Math.floor(Math.random() * 0xffffff);
            const counter = ObjectId.counter++;
            this.id[9] = (counter >> 16) & 0xff;
            this.id[10] = (counter >> 8) & 0xff;
            this.id[11] = counter & 0xff;
        } else if (typeof id === 'string') {
            if (id.length !== 24) {
                throw new Error('ObjectId string must be 24 hex characters');
            }
            this.id = Buffer.from(id, 'hex');
        } else if (Buffer.isBuffer(id)) {
            if (id.length !== 12) {
                throw new Error('ObjectId buffer must be 12 bytes');
            }
            this.id = id;
        } else {
            throw new Error('ObjectId must be string or Buffer');
        }
    }

    toString() {
        return this.id.toString('hex');
    }

    toHexString() {
        return this.toString();
    }

    equals(other) {
        if (!(other instanceof ObjectId)) return false;
        return this.id.equals(other.id);
    }
}

// Long class for 64-bit integers
export class Long {
    constructor(low, high = 0) {
        this.low = low | 0;
        this.high = high | 0;
    }

    static fromNumber(value) {
        return new Long(value | 0, (value / 0x100000000) | 0);
    }

    static fromBigInt(value) {
        const low = Number(value & 0xffffffffn);
        const high = Number((value >> 32n) & 0xffffffffn);
        return new Long(low, high);
    }

    toBigInt() {
        const highBits = BigInt(this.high >>> 0) << 32n;
        const lowBits = BigInt(this.low >>> 0);
        return highBits | lowBits;
    }

    toNumber() {
        return this.high * 0x100000000 + (this.low >>> 0);
    }
}

// Timestamp class for MongoDB timestamps
export class Timestamp extends Long {
    constructor(low, high) {
        super(low, high);
    }

    static fromTime(time) {
        const seconds = Math.floor(time / 1000);
        return new Timestamp(0, seconds);
    }
}

// Binary class for binary data
export class Binary {
    constructor(buffer, subtype = 0) {
        this.buffer = buffer;
        this.subtype = subtype;
    }
}

// BSON Serializer
export class BSONSerializer {
    static serialize(doc) {
        const buffers = [];
        const size = BSONSerializer.calculateObjectSize(doc);
        
        // Write document size
        const sizeBuffer = Buffer.alloc(4);
        sizeBuffer.writeInt32LE(size, 0);
        buffers.push(sizeBuffer);
        
        // Write document elements
        BSONSerializer.serializeObject(doc, buffers);
        
        // Write null terminator
        buffers.push(Buffer.from([0]));
        
        return Buffer.concat(buffers);
    }

    static serializeObject(obj, buffers) {
        for (const [key, value] of Object.entries(obj)) {
            BSONSerializer.serializeElement(key, value, buffers);
        }
    }

    static serializeElement(name, value, buffers) {
        // Determine type and write type byte
        let type;
        
        if (value === null) {
            type = BSONType.NULL;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
        } else if (value === undefined) {
            // Skip undefined values in objects
            return;
        } else if (typeof value === 'boolean') {
            type = BSONType.BOOLEAN;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            buffers.push(Buffer.from([value ? 1 : 0]));
        } else if (typeof value === 'number') {
            if (Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) {
                type = BSONType.INT32;
                buffers.push(Buffer.from([type]));
                BSONSerializer.writeCString(name, buffers);
                const buf = Buffer.alloc(4);
                buf.writeInt32LE(value, 0);
                buffers.push(buf);
            } else {
                type = BSONType.DOUBLE;
                buffers.push(Buffer.from([type]));
                BSONSerializer.writeCString(name, buffers);
                const buf = Buffer.alloc(8);
                buf.writeDoubleLE(value, 0);
                buffers.push(buf);
            }
        } else if (typeof value === 'string') {
            type = BSONType.STRING;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            BSONSerializer.writeString(value, buffers);
        } else if (typeof value === 'bigint') {
            type = BSONType.INT64;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeBigInt64LE(value, 0);
            buffers.push(buf);
        } else if (value instanceof ObjectId) {
            type = BSONType.OBJECTID;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            buffers.push(value.id);
        } else if (value instanceof Date) {
            type = BSONType.DATE;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeBigInt64LE(BigInt(value.getTime()), 0);
            buffers.push(buf);
        } else if (value instanceof Timestamp) {
            type = BSONType.TIMESTAMP;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeUInt32LE(value.low, 0);
            buf.writeUInt32LE(value.high, 4);
            buffers.push(buf);
        } else if (value instanceof Long) {
            type = BSONType.INT64;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeUInt32LE(value.low >>> 0, 0);
            buf.writeUInt32LE(value.high >>> 0, 4);
            buffers.push(buf);
        } else if (value instanceof Binary) {
            type = BSONType.BINARY;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            const sizeBuf = Buffer.alloc(4);
            sizeBuf.writeInt32LE(value.buffer.length, 0);
            buffers.push(sizeBuf);
            buffers.push(Buffer.from([value.subtype]));
            buffers.push(value.buffer);
        } else if (Array.isArray(value)) {
            type = BSONType.ARRAY;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            // Arrays are serialized as documents with numeric keys
            const arrayDoc = {};
            for (let i = 0; i < value.length; i++) {
                arrayDoc[i.toString()] = value[i];
            }
            const arrayBuffer = BSONSerializer.serialize(arrayDoc);
            buffers.push(arrayBuffer);
        } else if (value instanceof RegExp) {
            type = BSONType.REGEX;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            BSONSerializer.writeCString(value.source, buffers);
            let flags = '';
            if (value.ignoreCase) flags += 'i';
            if (value.multiline) flags += 'm';
            if (value.dotAll) flags += 's';
            if (value.unicode) flags += 'u';
            if (value.global) flags += 'g';
            BSONSerializer.writeCString(flags, buffers);
        } else if (typeof value === 'object') {
            type = BSONType.DOCUMENT;
            buffers.push(Buffer.from([type]));
            BSONSerializer.writeCString(name, buffers);
            const docBuffer = BSONSerializer.serialize(value);
            buffers.push(docBuffer);
        }
    }

    static writeCString(str, buffers) {
        buffers.push(Buffer.from(str, 'utf8'));
        buffers.push(Buffer.from([0]));
    }

    static writeString(str, buffers) {
        const strBuf = Buffer.from(str, 'utf8');
        const sizeBuf = Buffer.alloc(4);
        sizeBuf.writeInt32LE(strBuf.length + 1, 0);  // +1 for null terminator
        buffers.push(sizeBuf);
        buffers.push(strBuf);
        buffers.push(Buffer.from([0]));
    }

    static calculateObjectSize(obj) {
        let size = 4 + 1;  // Document size (4 bytes) + null terminator (1 byte)
        
        for (const [key, value] of Object.entries(obj)) {
            if (value === undefined) continue;  // Skip undefined
            
            size += 1;  // Type byte
            size += Buffer.byteLength(key, 'utf8') + 1;  // Key + null terminator
            
            if (value === null) {
                // No additional size
            } else if (typeof value === 'boolean') {
                size += 1;
            } else if (typeof value === 'number') {
                if (Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) {
                    size += 4;  // INT32
                } else {
                    size += 8;  // DOUBLE
                }
            } else if (typeof value === 'string') {
                size += 4 + Buffer.byteLength(value, 'utf8') + 1;
            } else if (typeof value === 'bigint') {
                size += 8;  // INT64
            } else if (value instanceof ObjectId) {
                size += 12;
            } else if (value instanceof Date) {
                size += 8;
            } else if (value instanceof Timestamp) {
                size += 8;
            } else if (value instanceof Long) {
                size += 8;
            } else if (value instanceof Binary) {
                size += 4 + 1 + value.buffer.length;  // size + subtype + data
            } else if (Array.isArray(value)) {
                const arrayDoc = {};
                for (let i = 0; i < value.length; i++) {
                    arrayDoc[i.toString()] = value[i];
                }
                size += BSONSerializer.calculateObjectSize(arrayDoc);
            } else if (value instanceof RegExp) {
                size += Buffer.byteLength(value.source, 'utf8') + 1;
                let flags = '';
                if (value.ignoreCase) flags += 'i';
                if (value.multiline) flags += 'm';
                if (value.dotAll) flags += 's';
                if (value.unicode) flags += 'u';
                if (value.global) flags += 'g';
                size += Buffer.byteLength(flags, 'utf8') + 1;
            } else if (typeof value === 'object') {
                size += BSONSerializer.calculateObjectSize(value);
            }
        }
        
        return size;
    }
}

// BSON Deserializer
export class BSONDeserializer {
    static deserialize(buffer, offset = 0) {
        if (buffer.length < offset + 4) {
            throw new Error(`Incomplete BSON document: buffer too small to read size`);
        }
        const size = buffer.readInt32LE(offset);
        if (buffer.length < offset + size) {
            throw new Error(`Incomplete BSON document: expected ${size} bytes, got ${buffer.length - offset}`);
        }
        
        const doc = {};
        let position = offset + 4;  // Skip size
        
        while (position < offset + size - 1) {  // -1 for null terminator
            const type = buffer[position++];
            if (type === 0) break;  // End of document
            
            const name = BSONDeserializer.readCString(buffer, position);
            position += name.length + 1;
            
            const result = BSONDeserializer.deserializeElement(type, buffer, position);
            doc[name] = result.value;
            position = result.position;
        }
        
        return doc;
    }

    static deserializeElement(type, buffer, position) {
        let value;
        
        switch (type) {
            case BSONType.DOUBLE:
                value = buffer.readDoubleLE(position);
                position += 8;
                break;
                
            case BSONType.STRING:
                const strSize = buffer.readInt32LE(position);
                position += 4;
                value = buffer.toString('utf8', position, position + strSize - 1);
                position += strSize;
                break;
                
            case BSONType.DOCUMENT:
                const docSize = buffer.readInt32LE(position);
                value = BSONDeserializer.deserialize(buffer, position);
                position += docSize;
                break;
                
            case BSONType.ARRAY:
                const arraySize = buffer.readInt32LE(position);
                const arrayDoc = BSONDeserializer.deserialize(buffer, position);
                position += arraySize;
                // Convert document with numeric keys back to array
                value = [];
                let i = 0;
                while (arrayDoc.hasOwnProperty(i.toString())) {
                    value.push(arrayDoc[i.toString()]);
                    i++;
                }
                break;
                
            case BSONType.BINARY:
                const binSize = buffer.readInt32LE(position);
                position += 4;
                const subtype = buffer[position++];
                const binData = buffer.slice(position, position + binSize);
                value = new Binary(binData, subtype);
                position += binSize;
                break;
                
            case BSONType.UNDEFINED:
                value = undefined;
                break;
                
            case BSONType.OBJECTID:
                value = new ObjectId(buffer.slice(position, position + 12));
                position += 12;
                break;
                
            case BSONType.BOOLEAN:
                value = buffer[position++] !== 0;
                break;
                
            case BSONType.DATE:
                const timestamp = buffer.readBigInt64LE(position);
                value = new Date(Number(timestamp));
                position += 8;
                break;
                
            case BSONType.NULL:
                value = null;
                break;
                
            case BSONType.REGEX:
                const pattern = BSONDeserializer.readCString(buffer, position);
                position += pattern.length + 1;
                const flags = BSONDeserializer.readCString(buffer, position);
                position += flags.length + 1;
                value = new RegExp(pattern, flags);
                break;
                
            case BSONType.INT32:
                value = buffer.readInt32LE(position);
                position += 4;
                break;
                
            case BSONType.TIMESTAMP:
                const low = buffer.readUInt32LE(position);
                const high = buffer.readUInt32LE(position + 4);
                value = new Timestamp(low, high);
                position += 8;
                break;
                
            case BSONType.INT64:
                const int64Low = buffer.readUInt32LE(position);
                const int64High = buffer.readUInt32LE(position + 4);
                value = new Long(int64Low, int64High);
                position += 8;
                break;
                
            case BSONType.MIN_KEY:
                value = { $minKey: 1 };
                break;
                
            case BSONType.MAX_KEY:
                value = { $maxKey: 1 };
                break;
                
            default:
                throw new Error(`Unsupported BSON type: 0x${type.toString(16)}`);
        }
        
        return { value, position };
    }

    static readCString(buffer, position) {
        let end = position;
        while (buffer[end] !== 0 && end < buffer.length) {
            end++;
        }
        return buffer.toString('utf8', position, end);
    }
}

// Main BSON interface
export const BSON = {
    serialize: (doc) => BSONSerializer.serialize(doc),
    deserialize: (buffer) => BSONDeserializer.deserialize(buffer),
    ObjectId,
    Long,
    Timestamp,
    Binary
};

// Export everything
export default BSON;