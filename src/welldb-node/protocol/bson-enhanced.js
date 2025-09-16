/**
 * Enhanced BSON Serialization/Deserialization for Full MongoDB Compatibility
 * Implements all BSON types according to http://bsonspec.org/
 */

// BSON Type codes
export const BSONType = {
    DOUBLE: 0x01,          // 64-bit floating point
    STRING: 0x02,          // UTF-8 string
    DOCUMENT: 0x03,        // Embedded document
    ARRAY: 0x04,           // Array
    BINARY: 0x05,          // Binary data
    UNDEFINED: 0x06,       // Deprecated
    OBJECTID: 0x07,        // ObjectId
    BOOLEAN: 0x08,         // Boolean
    DATE: 0x09,            // UTC datetime
    NULL: 0x0A,            // Null value
    REGEX: 0x0B,           // Regular expression
    DBPOINTER: 0x0C,       // Deprecated
    CODE: 0x0D,            // JavaScript code
    SYMBOL: 0x0E,          // Deprecated
    CODE_W_SCOPE: 0x0F,    // JavaScript code with scope
    INT32: 0x10,           // 32-bit integer
    TIMESTAMP: 0x11,       // MongoDB internal timestamp
    INT64: 0x12,           // 64-bit integer
    DECIMAL128: 0x13,      // 128-bit decimal
    MIN_KEY: 0xFF,         // Min key
    MAX_KEY: 0x7F          // Max key
};

// Binary subtypes
export const BinarySubtype = {
    GENERIC: 0x00,         // Generic binary
    FUNCTION: 0x01,        // Function
    BINARY_OLD: 0x02,      // Binary (deprecated)
    UUID_OLD: 0x03,        // UUID (deprecated)
    UUID: 0x04,            // UUID
    MD5: 0x05,             // MD5 hash
    ENCRYPTED: 0x06,       // Encrypted BSON value
    COMPRESSED: 0x07,      // Compressed BSON column
    USER_DEFINED: 0x80     // User defined
};

// ObjectId class
export class ObjectId {
    static _counter = Math.floor(Math.random() * 0xffffff);
    static _process = Math.floor(Math.random() * 0xffff);

    constructor(id) {
        if (!id) {
            // Generate new ObjectId
            this.id = Buffer.alloc(12);
            // 4-byte timestamp
            this.id.writeUInt32BE(Math.floor(Date.now() / 1000), 0);
            // 5-byte random value (3 bytes machine + 2 bytes process)
            const machineId = Math.floor(Math.random() * 0xffffff);
            this.id[4] = (machineId >> 16) & 0xff;
            this.id[5] = (machineId >> 8) & 0xff;
            this.id[6] = machineId & 0xff;
            this.id[7] = (ObjectId._process >> 8) & 0xff;
            this.id[8] = ObjectId._process & 0xff;
            // 3-byte incrementing counter
            const counter = ObjectId._counter++;
            this.id[9] = (counter >> 16) & 0xff;
            this.id[10] = (counter >> 8) & 0xff;
            this.id[11] = counter & 0xff;
        } else if (typeof id === 'string') {
            if (!/^[0-9a-fA-F]{24}$/.test(id)) {
                throw new Error('Invalid ObjectId string');
            }
            this.id = Buffer.from(id, 'hex');
        } else if (Buffer.isBuffer(id)) {
            if (id.length !== 12) {
                throw new Error('ObjectId buffer must be 12 bytes');
            }
            this.id = Buffer.from(id);
        } else {
            throw new Error('ObjectId must be string, Buffer, or undefined');
        }
    }

    toString() {
        return this.id.toString('hex');
    }

    toHexString() {
        return this.toString();
    }

    toJSON() {
        return this.toString();
    }

    equals(other) {
        if (!(other instanceof ObjectId)) return false;
        return this.id.equals(other.id);
    }

    getTimestamp() {
        return new Date(this.id.readUInt32BE(0) * 1000);
    }
}

// Long class for 64-bit integers
export class Long {
    constructor(low, high = 0, unsigned = false) {
        this.low = low | 0;
        this.high = high | 0;
        this.unsigned = !!unsigned;
    }

    static fromNumber(value, unsigned = false) {
        if (unsigned) {
            return new Long(value >>> 0, (value / 0x100000000) >>> 0, true);
        }
        return new Long(value | 0, (value / 0x100000000) | 0, false);
    }

    static fromBigInt(value, unsigned = false) {
        const low = Number(value & 0xffffffffn);
        const high = Number((value >> 32n) & 0xffffffffn);
        return new Long(low, high, unsigned);
    }

    static fromBits(low, high, unsigned = false) {
        return new Long(low, high, unsigned);
    }

    static ZERO = new Long(0, 0);
    static ONE = new Long(1, 0);
    static NEG_ONE = new Long(-1, -1);

    toBigInt() {
        if (this.unsigned) {
            const highBits = BigInt(this.high >>> 0) << 32n;
            const lowBits = BigInt(this.low >>> 0);
            return highBits | lowBits;
        }
        const highBits = BigInt(this.high) << 32n;
        const lowBits = BigInt(this.low >>> 0);
        return highBits | lowBits;
    }

    toNumber() {
        if (this.unsigned) {
            return this.high * 0x100000000 + (this.low >>> 0);
        }
        return this.high * 0x100000000 + (this.low >>> 0);
    }

    toString(radix = 10) {
        return this.toBigInt().toString(radix);
    }

    toJSON() {
        return this.toString();
    }

    equals(other) {
        if (!(other instanceof Long)) return false;
        return this.low === other.low && this.high === other.high && this.unsigned === other.unsigned;
    }
}

// Timestamp class for MongoDB timestamps
export class Timestamp extends Long {
    constructor(low, high) {
        super(low, high, true);
    }

    static fromTime(time) {
        const seconds = Math.floor(time / 1000);
        return new Timestamp(0, seconds);
    }

    static fromBits(low, high) {
        return new Timestamp(low, high);
    }

    getLowBits() {
        return this.low;
    }

    getHighBits() {
        return this.high;
    }
}

// Binary class for binary data
export class Binary {
    constructor(buffer, subtype = BinarySubtype.GENERIC) {
        if (typeof buffer === 'string') {
            this.buffer = Buffer.from(buffer, 'base64');
        } else if (Buffer.isBuffer(buffer)) {
            this.buffer = Buffer.from(buffer);
        } else if (ArrayBuffer.isView(buffer)) {
            this.buffer = Buffer.from(buffer.buffer, buffer.byteOffset, buffer.byteLength);
        } else {
            throw new Error('Binary data must be Buffer, string, or TypedArray');
        }
        this.subtype = subtype;
    }

    length() {
        return this.buffer.length;
    }

    toBase64() {
        return this.buffer.toString('base64');
    }

    toString(encoding = 'base64') {
        return this.buffer.toString(encoding);
    }

    toJSON() {
        return {
            $binary: {
                base64: this.toBase64(),
                subType: this.subtype.toString(16).padStart(2, '0')
            }
        };
    }
}

// UUID class
export class UUID {
    constructor(id) {
        if (!id) {
            // Generate new UUID v4
            this.id = Buffer.alloc(16);
            for (let i = 0; i < 16; i++) {
                this.id[i] = Math.floor(Math.random() * 256);
            }
            // Set version (4) and variant bits
            this.id[6] = (this.id[6] & 0x0f) | 0x40;
            this.id[8] = (this.id[8] & 0x3f) | 0x80;
        } else if (typeof id === 'string') {
            // Parse UUID string
            const clean = id.replace(/-/g, '');
            if (!/^[0-9a-fA-F]{32}$/.test(clean)) {
                throw new Error('Invalid UUID string');
            }
            this.id = Buffer.from(clean, 'hex');
        } else if (Buffer.isBuffer(id)) {
            if (id.length !== 16) {
                throw new Error('UUID buffer must be 16 bytes');
            }
            this.id = Buffer.from(id);
        } else {
            throw new Error('UUID must be string, Buffer, or undefined');
        }
    }

    toString() {
        const hex = this.id.toString('hex');
        return [
            hex.substring(0, 8),
            hex.substring(8, 12),
            hex.substring(12, 16),
            hex.substring(16, 20),
            hex.substring(20, 32)
        ].join('-');
    }

    toBinary() {
        return new Binary(this.id, BinarySubtype.UUID);
    }

    equals(other) {
        if (!(other instanceof UUID)) return false;
        return this.id.equals(other.id);
    }
}

// Decimal128 class for high-precision decimals
export class Decimal128 {
    constructor(value) {
        this.bytes = Buffer.alloc(16);
        
        if (typeof value === 'string') {
            // Parse decimal string
            this._parseString(value);
        } else if (Buffer.isBuffer(value)) {
            if (value.length !== 16) {
                throw new Error('Decimal128 buffer must be 16 bytes');
            }
            this.bytes = Buffer.from(value);
        } else if (typeof value === 'number') {
            // Convert number to Decimal128
            this._parseString(value.toString());
        } else {
            throw new Error('Decimal128 must be string, number, or Buffer');
        }
    }

    _parseString(str) {
        // Simplified Decimal128 parsing
        // In production, use a proper IEEE 754-2008 decimal128 implementation
        const num = parseFloat(str);
        if (isNaN(num)) {
            // NaN representation
            this.bytes[15] = 0x7c;
        } else if (!isFinite(num)) {
            // Infinity representation
            this.bytes[15] = num > 0 ? 0x78 : 0xf8;
        } else {
            // Store as simplified representation (not fully IEEE 754-2008 compliant)
            // This is a placeholder - real implementation would properly encode the decimal
            const buffer = Buffer.alloc(16);
            buffer.writeDoubleLE(num, 0);
            this.bytes = buffer;
        }
    }

    toString() {
        // Simplified conversion back to string
        // In production, properly decode IEEE 754-2008 decimal128
        if (this.bytes[15] === 0x7c) return 'NaN';
        if (this.bytes[15] === 0x78) return 'Infinity';
        if (this.bytes[15] === 0xf8) return '-Infinity';
        return this.bytes.readDoubleLE(0).toString();
    }

    toJSON() {
        return { $numberDecimal: this.toString() };
    }
}

// MinKey class
export class MinKey {
    constructor() {}
    
    toJSON() {
        return { $minKey: 1 };
    }
}

// MaxKey class
export class MaxKey {
    constructor() {}
    
    toJSON() {
        return { $maxKey: 1 };
    }
}

// Code class for JavaScript code
export class Code {
    constructor(code, scope = null) {
        this.code = code;
        this.scope = scope;
    }

    toJSON() {
        if (this.scope) {
            return { $code: this.code, $scope: this.scope };
        }
        return { $code: this.code };
    }
}

// DBRef class for database references
export class DBRef {
    constructor(collection, id, db = null) {
        this.collection = collection;
        this.oid = id;
        this.db = db;
    }

    toJSON() {
        const ref = { $ref: this.collection, $id: this.oid };
        if (this.db) ref.$db = this.db;
        return ref;
    }
}

// Double class for explicit double values
export class Double {
    constructor(value) {
        this.value = +value;
    }

    valueOf() {
        return this.value;
    }

    toString() {
        return this.value.toString();
    }

    toJSON() {
        return this.value;
    }
}

// Int32 class for explicit 32-bit integers
export class Int32 {
    constructor(value) {
        this.value = value | 0;
    }

    valueOf() {
        return this.value;
    }

    toString() {
        return this.value.toString();
    }

    toJSON() {
        return this.value;
    }
}

// Export BSON namespace with serialize/deserialize functions
export const BSON = {
    serialize,
    deserialize,
    ObjectId,
    Long,
    Timestamp,
    Binary,
    UUID,
    Decimal128,
    MinKey,
    MaxKey,
    Code,
    DBRef,
    Double,
    Int32,
    BinarySubtype
};

// Main serialize function
export function serialize(doc) {
    const serializer = new BSONSerializer();
    return serializer.serialize(doc);
}

// Main deserialize function
export function deserialize(buffer, options = {}) {
    const deserializer = new BSONDeserializer(options);
    return deserializer.deserialize(buffer, 0);
}

// BSON Serializer class
class BSONSerializer {
    serialize(doc) {
        const buffers = [];
        const size = this.calculateObjectSize(doc);
        
        // Write document size
        const sizeBuffer = Buffer.alloc(4);
        sizeBuffer.writeInt32LE(size, 0);
        buffers.push(sizeBuffer);
        
        // Write document elements
        this.serializeObject(doc, buffers);
        
        // Write null terminator
        buffers.push(Buffer.from([0]));
        
        return Buffer.concat(buffers);
    }

    serializeObject(obj, buffers) {
        for (const [key, value] of Object.entries(obj)) {
            this.serializeElement(key, value, buffers);
        }
    }

    serializeElement(name, value, buffers) {
        // Skip undefined values
        if (value === undefined) return;
        
        // Handle null
        if (value === null) {
            buffers.push(Buffer.from([BSONType.NULL]));
            this.writeCString(name, buffers);
            return;
        }
        
        // Handle special types
        if (value instanceof MinKey) {
            buffers.push(Buffer.from([BSONType.MIN_KEY]));
            this.writeCString(name, buffers);
            return;
        }
        
        if (value instanceof MaxKey) {
            buffers.push(Buffer.from([BSONType.MAX_KEY]));
            this.writeCString(name, buffers);
            return;
        }
        
        if (value instanceof ObjectId) {
            buffers.push(Buffer.from([BSONType.OBJECTID]));
            this.writeCString(name, buffers);
            buffers.push(value.id);
            return;
        }
        
        if (value instanceof Date) {
            buffers.push(Buffer.from([BSONType.DATE]));
            this.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeBigInt64LE(BigInt(value.getTime()), 0);
            buffers.push(buf);
            return;
        }
        
        if (value instanceof Timestamp) {
            buffers.push(Buffer.from([BSONType.TIMESTAMP]));
            this.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeUInt32LE(value.low, 0);
            buf.writeUInt32LE(value.high, 4);
            buffers.push(buf);
            return;
        }
        
        if (value instanceof Long) {
            buffers.push(Buffer.from([BSONType.INT64]));
            this.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeInt32LE(value.low, 0);
            buf.writeInt32LE(value.high, 4);
            buffers.push(buf);
            return;
        }
        
        if (value instanceof Decimal128) {
            buffers.push(Buffer.from([BSONType.DECIMAL128]));
            this.writeCString(name, buffers);
            buffers.push(value.bytes);
            return;
        }
        
        if (value instanceof Binary || value instanceof UUID) {
            const binary = value instanceof UUID ? value.toBinary() : value;
            buffers.push(Buffer.from([BSONType.BINARY]));
            this.writeCString(name, buffers);
            const sizeBuf = Buffer.alloc(4);
            sizeBuf.writeInt32LE(binary.buffer.length, 0);
            buffers.push(sizeBuf);
            buffers.push(Buffer.from([binary.subtype]));
            buffers.push(binary.buffer);
            return;
        }
        
        if (value instanceof Code) {
            if (value.scope) {
                buffers.push(Buffer.from([BSONType.CODE_W_SCOPE]));
                this.writeCString(name, buffers);
                
                // Calculate total size
                const codeBytes = Buffer.from(value.code, 'utf8');
                const scopeBytes = this.serialize(value.scope);
                const totalSize = 4 + 4 + codeBytes.length + 1 + scopeBytes.length;
                
                const sizeBuf = Buffer.alloc(4);
                sizeBuf.writeInt32LE(totalSize, 0);
                buffers.push(sizeBuf);
                
                // Write code string
                const codeSizeBuf = Buffer.alloc(4);
                codeSizeBuf.writeInt32LE(codeBytes.length + 1, 0);
                buffers.push(codeSizeBuf);
                buffers.push(codeBytes);
                buffers.push(Buffer.from([0]));
                
                // Write scope document
                buffers.push(scopeBytes);
            } else {
                buffers.push(Buffer.from([BSONType.CODE]));
                this.writeCString(name, buffers);
                this.writeString(value.code, buffers);
            }
            return;
        }
        
        if (value instanceof Double) {
            buffers.push(Buffer.from([BSONType.DOUBLE]));
            this.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeDoubleLE(value.value, 0);
            buffers.push(buf);
            return;
        }
        
        if (value instanceof Int32) {
            buffers.push(Buffer.from([BSONType.INT32]));
            this.writeCString(name, buffers);
            const buf = Buffer.alloc(4);
            buf.writeInt32LE(value.value, 0);
            buffers.push(buf);
            return;
        }
        
        // Handle primitive types
        if (typeof value === 'boolean') {
            buffers.push(Buffer.from([BSONType.BOOLEAN]));
            this.writeCString(name, buffers);
            buffers.push(Buffer.from([value ? 1 : 0]));
            return;
        }
        
        if (typeof value === 'number') {
            if (Number.isInteger(value) && value >= -2147483648 && value <= 2147483647) {
                buffers.push(Buffer.from([BSONType.INT32]));
                this.writeCString(name, buffers);
                const buf = Buffer.alloc(4);
                buf.writeInt32LE(value, 0);
                buffers.push(buf);
            } else {
                buffers.push(Buffer.from([BSONType.DOUBLE]));
                this.writeCString(name, buffers);
                const buf = Buffer.alloc(8);
                buf.writeDoubleLE(value, 0);
                buffers.push(buf);
            }
            return;
        }
        
        if (typeof value === 'string') {
            buffers.push(Buffer.from([BSONType.STRING]));
            this.writeCString(name, buffers);
            this.writeString(value, buffers);
            return;
        }
        
        if (typeof value === 'bigint') {
            buffers.push(Buffer.from([BSONType.INT64]));
            this.writeCString(name, buffers);
            const buf = Buffer.alloc(8);
            buf.writeBigInt64LE(value, 0);
            buffers.push(buf);
            return;
        }
        
        if (value instanceof RegExp) {
            buffers.push(Buffer.from([BSONType.REGEX]));
            this.writeCString(name, buffers);
            this.writeCString(value.source, buffers);
            let flags = '';
            if (value.ignoreCase) flags += 'i';
            if (value.multiline) flags += 'm';
            if (value.dotAll) flags += 's';
            if (value.unicode) flags += 'u';
            if (value.global) flags += 'g';
            this.writeCString(flags, buffers);
            return;
        }
        
        if (Array.isArray(value)) {
            buffers.push(Buffer.from([BSONType.ARRAY]));
            this.writeCString(name, buffers);
            // Arrays are serialized as documents with numeric keys
            const arrayDoc = {};
            for (let i = 0; i < value.length; i++) {
                arrayDoc[i.toString()] = value[i];
            }
            const arrayBuffer = this.serialize(arrayDoc);
            buffers.push(arrayBuffer);
            return;
        }
        
        // Handle plain objects
        if (typeof value === 'object') {
            buffers.push(Buffer.from([BSONType.DOCUMENT]));
            this.writeCString(name, buffers);
            const docBuffer = this.serialize(value);
            buffers.push(docBuffer);
            return;
        }
    }

    writeCString(str, buffers) {
        buffers.push(Buffer.from(str, 'utf8'));
        buffers.push(Buffer.from([0]));
    }

    writeString(str, buffers) {
        const strBuf = Buffer.from(str, 'utf8');
        const sizeBuf = Buffer.alloc(4);
        sizeBuf.writeInt32LE(strBuf.length + 1, 0);
        buffers.push(sizeBuf);
        buffers.push(strBuf);
        buffers.push(Buffer.from([0]));
    }

    calculateObjectSize(obj) {
        let size = 4 + 1; // Document size (4 bytes) + null terminator (1 byte)
        
        for (const [key, value] of Object.entries(obj)) {
            if (value === undefined) continue;
            
            size += 1; // Type byte
            size += Buffer.byteLength(key, 'utf8') + 1; // Key + null terminator
            
            if (value === null || value instanceof MinKey || value instanceof MaxKey) {
                // No additional size
            } else if (typeof value === 'boolean') {
                size += 1;
            } else if (typeof value === 'number' || value instanceof Int32) {
                const num = typeof value === 'number' ? value : value.value;
                if (Number.isInteger(num) && num >= -2147483648 && num <= 2147483647) {
                    size += 4;
                } else {
                    size += 8;
                }
            } else if (value instanceof Double) {
                size += 8;
            } else if (typeof value === 'string') {
                size += 4 + Buffer.byteLength(value, 'utf8') + 1;
            } else if (typeof value === 'bigint') {
                size += 8;
            } else if (value instanceof ObjectId) {
                size += 12;
            } else if (value instanceof Date) {
                size += 8;
            } else if (value instanceof Timestamp || value instanceof Long) {
                size += 8;
            } else if (value instanceof Decimal128) {
                size += 16;
            } else if (value instanceof Binary || value instanceof UUID) {
                const binary = value instanceof UUID ? value.toBinary() : value;
                size += 4 + 1 + binary.buffer.length;
            } else if (value instanceof Code) {
                if (value.scope) {
                    const codeSize = Buffer.byteLength(value.code, 'utf8');
                    const scopeSize = this.calculateObjectSize(value.scope);
                    size += 4 + 4 + codeSize + 1 + scopeSize;
                } else {
                    size += 4 + Buffer.byteLength(value.code, 'utf8') + 1;
                }
            } else if (Array.isArray(value)) {
                const arrayDoc = {};
                for (let i = 0; i < value.length; i++) {
                    arrayDoc[i.toString()] = value[i];
                }
                size += this.calculateObjectSize(arrayDoc);
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
                size += this.calculateObjectSize(value);
            }
        }
        
        return size;
    }
}

// BSON Deserializer class
class BSONDeserializer {
    constructor(options = {}) {
        this.promoteBuffers = options.promoteBuffers || false;
        this.promoteLongs = options.promoteLongs !== undefined ? options.promoteLongs : true;
        this.promoteValues = options.promoteValues !== undefined ? options.promoteValues : true;
    }

    deserialize(buffer, offset = 0) {
        if (buffer.length < offset + 4) {
            throw new Error('Incomplete BSON document: buffer too small');
        }
        
        const size = buffer.readInt32LE(offset);
        if (buffer.length < offset + size) {
            throw new Error(`Incomplete BSON document: expected ${size} bytes, got ${buffer.length - offset}`);
        }
        
        const doc = {};
        let position = offset + 4;
        
        while (position < offset + size - 1) {
            const type = buffer[position++];
            if (type === 0) break; // End of document
            
            const name = this.readCString(buffer, position);
            position += Buffer.byteLength(name, 'utf8') + 1;
            
            const result = this.deserializeElement(type, buffer, position);
            doc[name] = result.value;
            position = result.position;
        }
        
        return doc;
    }

    deserializeElement(type, buffer, position) {
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
                value = this.deserialize(buffer, position);
                position += docSize;
                break;
                
            case BSONType.ARRAY:
                const arraySize = buffer.readInt32LE(position);
                const arrayDoc = this.deserialize(buffer, position);
                position += arraySize;
                // Convert to array
                value = [];
                for (let i = 0; i in arrayDoc; i++) {
                    value.push(arrayDoc[i.toString()]);
                }
                break;
                
            case BSONType.BINARY:
                const binSize = buffer.readInt32LE(position);
                position += 4;
                const subtype = buffer[position++];
                const binData = buffer.slice(position, position + binSize);
                position += binSize;
                
                if (subtype === BinarySubtype.UUID || subtype === BinarySubtype.UUID_OLD) {
                    value = new UUID(binData);
                } else {
                    value = new Binary(binData, subtype);
                }
                break;
                
            case BSONType.UNDEFINED:
                value = undefined;
                break;
                
            case BSONType.OBJECTID:
                value = new ObjectId(buffer.slice(position, position + 12));
                position += 12;
                break;
                
            case BSONType.BOOLEAN:
                value = buffer[position++] === 1;
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
                const pattern = this.readCString(buffer, position);
                position += Buffer.byteLength(pattern, 'utf8') + 1;
                const flags = this.readCString(buffer, position);
                position += Buffer.byteLength(flags, 'utf8') + 1;
                value = new RegExp(pattern, flags);
                break;
                
            case BSONType.DBPOINTER:
                // Deprecated - skip
                const dbPtrStrSize = buffer.readInt32LE(position);
                position += 4 + dbPtrStrSize + 12; // string + ObjectId
                value = null;
                break;
                
            case BSONType.CODE:
                const codeSize = buffer.readInt32LE(position);
                position += 4;
                const code = buffer.toString('utf8', position, position + codeSize - 1);
                position += codeSize;
                value = new Code(code);
                break;
                
            case BSONType.SYMBOL:
                // Deprecated - treat as string
                const symSize = buffer.readInt32LE(position);
                position += 4;
                value = buffer.toString('utf8', position, position + symSize - 1);
                position += symSize;
                break;
                
            case BSONType.CODE_W_SCOPE:
                const totalSize = buffer.readInt32LE(position);
                position += 4;
                const codeWsSize = buffer.readInt32LE(position);
                position += 4;
                const codeWs = buffer.toString('utf8', position, position + codeWsSize - 1);
                position += codeWsSize;
                const scopeDoc = this.deserialize(buffer, position);
                const scopeSize = buffer.readInt32LE(position);
                position += scopeSize;
                value = new Code(codeWs, scopeDoc);
                break;
                
            case BSONType.INT32:
                value = buffer.readInt32LE(position);
                if (!this.promoteValues) {
                    value = new Int32(value);
                }
                position += 4;
                break;
                
            case BSONType.TIMESTAMP:
                const low = buffer.readUInt32LE(position);
                const high = buffer.readUInt32LE(position + 4);
                value = new Timestamp(low, high);
                position += 8;
                break;
                
            case BSONType.INT64:
                const int64Low = buffer.readInt32LE(position);
                const int64High = buffer.readInt32LE(position + 4);
                value = new Long(int64Low, int64High);
                if (this.promoteLongs) {
                    const num = value.toNumber();
                    if (Number.isSafeInteger(num)) {
                        value = num;
                    }
                }
                position += 8;
                break;
                
            case BSONType.DECIMAL128:
                value = new Decimal128(buffer.slice(position, position + 16));
                position += 16;
                break;
                
            case BSONType.MIN_KEY:
                value = new MinKey();
                break;
                
            case BSONType.MAX_KEY:
                value = new MaxKey();
                break;
                
            default:
                throw new Error(`Unknown BSON type: 0x${type.toString(16)}`);
        }
        
        return { value, position };
    }

    readCString(buffer, position) {
        const start = position;
        while (buffer[position] !== 0) position++;
        return buffer.toString('utf8', start, position);
    }
}

// Export everything for compatibility
export default BSON;