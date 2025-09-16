/**
 * Prompt Analyzer Module
 * Natural Language Processing for analyzing prompts and extracting schema information
 * Implements basic NLP from scratch without third-party dependencies
 */

import logger from '../utils/logger.js';

const log = logger.child('PromptAnalyzer');

export class PromptAnalyzer {
    constructor() {
        // Common data type keywords
        this.dataTypeKeywords = {
            string: ['string', 'text', 'name', 'title', 'description', 'label', 'message', 'content', 'varchar', 'char'],
            number: ['number', 'numeric', 'float', 'double', 'decimal', 'amount', 'price', 'cost', 'value', 'score', 'rating'],
            integer: ['integer', 'int', 'count', 'quantity', 'age', 'year', 'month', 'day', 'id', 'index'],
            boolean: ['boolean', 'bool', 'flag', 'yes', 'no', 'true', 'false', 'active', 'enabled', 'disabled'],
            date: ['date', 'datetime', 'timestamp', 'time', 'created', 'updated', 'modified', 'birth', 'expiry'],
            email: ['email', 'mail', 'address', 'contact'],
            url: ['url', 'link', 'website', 'uri', 'href', 'path'],
            uuid: ['uuid', 'guid', 'identifier', 'uid'],
            array: ['array', 'list', 'items', 'collection', 'multiple', 'many', 'several'],
            object: ['object', 'entity', 'model', 'record', 'document', 'profile', 'data']
        };
        
        // Common constraint keywords
        this.constraintKeywords = {
            required: ['required', 'mandatory', 'must', 'need', 'necessary', 'essential'],
            optional: ['optional', 'maybe', 'might', 'can', 'could', 'possible'],
            unique: ['unique', 'distinct', 'different', 'exclusive'],
            minMax: ['minimum', 'maximum', 'min', 'max', 'least', 'most', 'between', 'range'],
            length: ['length', 'characters', 'chars', 'letters', 'digits'],
            pattern: ['pattern', 'format', 'match', 'regex', 'expression']
        };
        
        // Domain-specific patterns
        this.domainPatterns = {
            user: ['user', 'account', 'profile', 'member', 'customer', 'client'],
            product: ['product', 'item', 'merchandise', 'goods', 'article', 'sku'],
            order: ['order', 'purchase', 'transaction', 'sale', 'invoice', 'receipt'],
            payment: ['payment', 'billing', 'charge', 'fee', 'amount', 'total'],
            address: ['address', 'location', 'street', 'city', 'state', 'zip', 'country'],
            company: ['company', 'organization', 'business', 'enterprise', 'firm'],
            medical: ['patient', 'doctor', 'diagnosis', 'treatment', 'medication', 'health'],
            education: ['student', 'course', 'grade', 'teacher', 'class', 'subject']
        };
    }
    
    /**
     * Analyze a prompt and extract schema information
     */
    analyze(prompt) {
        log.debug('Analyzing prompt:', prompt);
        
        const normalized = this.normalizeText(prompt);
        const tokens = this.tokenize(normalized);
        const sentences = this.splitSentences(prompt);
        
        // Extract various features
        const entities = this.extractEntities(tokens, sentences);
        const fields = this.extractFields(tokens, sentences);
        const constraints = this.extractConstraints(tokens, sentences);
        const relationships = this.extractRelationships(tokens, sentences);
        const domain = this.detectDomain(tokens);
        
        // Build schema from extracted information
        const schema = this.buildSchema(entities, fields, constraints, relationships, domain);
        
        log.debug('Analysis complete:', {
            entities: entities.length,
            fields: fields.length,
            constraints: Object.keys(constraints).length,
            domain
        });
        
        return {
            schema,
            metadata: {
                entities,
                fields,
                constraints,
                relationships,
                domain,
                confidence: this.calculateConfidence(entities, fields)
            }
        };
    }
    
    /**
     * Normalize text for processing
     */
    normalizeText(text) {
        return text
            .toLowerCase()
            .replace(/[^\w\s\-_]/g, ' ')  // Remove special chars except - and _
            .replace(/\s+/g, ' ')          // Normalize whitespace
            .trim();
    }
    
    /**
     * Simple tokenization
     */
    tokenize(text) {
        return text.split(/\s+/).filter(token => token.length > 0);
    }
    
    /**
     * Split text into sentences
     */
    splitSentences(text) {
        return text
            .split(/[.!?]+/)
            .map(s => s.trim())
            .filter(s => s.length > 0);
    }
    
    /**
     * Extract entities (nouns that could be collections/models)
     */
    extractEntities(tokens, sentences) {
        const entities = new Set();
        
        // Look for entity indicators
        const entityIndicators = ['model', 'collection', 'table', 'entity', 'type', 'class', 'schema'];
        
        for (let i = 0; i < tokens.length; i++) {
            const token = tokens[i];
            
            // Check if token is an entity indicator
            if (entityIndicators.includes(token) && i + 1 < tokens.length) {
                const nextToken = tokens[i + 1];
                if (this.isNoun(nextToken)) {
                    entities.add(this.singularize(nextToken));
                }
            }
            
            // Check for "a/an X" pattern
            if ((token === 'a' || token === 'an') && i + 1 < tokens.length) {
                const nextToken = tokens[i + 1];
                if (this.isNoun(nextToken)) {
                    entities.add(this.singularize(nextToken));
                }
            }
            
            // Check for domain-specific entities
            for (const [domain, keywords] of Object.entries(this.domainPatterns)) {
                if (keywords.includes(token)) {
                    entities.add(this.singularize(token));
                }
            }
        }
        
        return Array.from(entities);
    }
    
    /**
     * Extract fields from the prompt
     */
    extractFields(tokens, sentences) {
        const fields = [];
        
        // Look for field patterns
        for (const sentence of sentences) {
            const sentTokens = this.tokenize(this.normalizeText(sentence));
            
            // Pattern: "with/has/contains X, Y, and Z"
            const withIndex = sentTokens.findIndex(t => ['with', 'has', 'contains', 'including'].includes(t));
            if (withIndex >= 0) {
                const fieldTokens = sentTokens.slice(withIndex + 1);
                const extracted = this.extractListItems(fieldTokens);
                
                for (const item of extracted) {
                    const field = this.parseField(item);
                    if (field) fields.push(field);
                }
            }
            
            // Pattern: "X field/property/attribute"
            for (let i = 0; i < sentTokens.length - 1; i++) {
                if (['field', 'property', 'attribute', 'column'].includes(sentTokens[i + 1])) {
                    const field = this.parseField(sentTokens[i]);
                    if (field) fields.push(field);
                }
            }
            
            // Also look for simple comma-separated lists after entity names
            // Pattern: "model with X, Y, and Z" or "schema with X, Y, Z"
            const modelWords = ['model', 'schema', 'entity', 'collection', 'table', 'record'];
            for (let i = 0; i < sentTokens.length; i++) {
                if (modelWords.includes(sentTokens[i]) && i + 1 < sentTokens.length) {
                    // Skip the next word (entity name) and look for "with"
                    let j = i + 1;
                    while (j < sentTokens.length && !['with', 'has', 'contains'].includes(sentTokens[j])) {
                        j++;
                    }
                    if (j < sentTokens.length) {
                        const remainingTokens = sentTokens.slice(j + 1);
                        const extracted = this.extractListItems(remainingTokens);
                        for (const item of extracted) {
                            const field = this.parseField(item);
                            if (field) fields.push(field);
                        }
                        break; // Process once per sentence
                    }
                }
            }
        }
        
        // Deduplicate fields by name
        const uniqueFields = new Map();
        for (const field of fields) {
            if (!uniqueFields.has(field.name)) {
                uniqueFields.set(field.name, field);
            }
        }
        
        return Array.from(uniqueFields.values());
    }
    
    /**
     * Parse a field description
     */
    parseField(text) {
        if (!text || typeof text !== 'string') return null;
        
        const tokens = text.split(/[\s_-]+/).filter(t => t.length > 0);
        if (tokens.length === 0) return null;
        
        // For simple field names (just one word), use that as the name
        let name = tokens.length === 1 ? tokens[0] : tokens[tokens.length - 1];
        let type = 'string';  // Default type
        
        // Detect type from tokens
        for (const token of tokens) {
            for (const [dataType, keywords] of Object.entries(this.dataTypeKeywords)) {
                if (keywords.includes(token.toLowerCase())) {
                    type = dataType;
                    // If we found a type keyword and it's not the field name, update
                    if (tokens.length > 1 && token !== name) {
                        // Remove type from tokens to get cleaner field name
                        const nameTokens = tokens.filter(t => t !== token);
                        if (nameTokens.length > 0) {
                            name = nameTokens[nameTokens.length - 1];
                        }
                    }
                    break;
                }
            }
        }
        
        // Infer type from field name if not detected
        if (type === 'string') {
            type = this.inferTypeFromName(name);
        }
        
        // Clean up the name
        name = name.replace(/[^\w]/g, '');
        if (!name) return null;
        
        return {
            name: this.camelCase(name),
            type,
            description: text
        };
    }
    
    /**
     * Extract constraints from the prompt
     */
    extractConstraints(tokens, sentences) {
        const constraints = {};
        
        for (const sentence of sentences) {
            const sentTokens = this.tokenize(this.normalizeText(sentence));
            
            // Required fields
            for (const token of sentTokens) {
                if (this.constraintKeywords.required.includes(token)) {
                    constraints.required = true;
                }
            }
            
            // Min/Max constraints
            for (let i = 0; i < sentTokens.length - 1; i++) {
                const token = sentTokens[i];
                const nextToken = sentTokens[i + 1];
                
                if (this.constraintKeywords.minMax.includes(token)) {
                    const number = this.extractNumber(nextToken);
                    if (number !== null) {
                        if (token.includes('min')) {
                            constraints.minimum = number;
                        } else if (token.includes('max')) {
                            constraints.maximum = number;
                        }
                    }
                }
                
                // "between X and Y" pattern
                if (token === 'between' && i + 3 < sentTokens.length) {
                    const min = this.extractNumber(sentTokens[i + 1]);
                    const max = this.extractNumber(sentTokens[i + 3]);
                    if (min !== null && max !== null) {
                        constraints.minimum = min;
                        constraints.maximum = max;
                    }
                }
            }
            
            // Length constraints
            const lengthMatch = sentence.match(/(\d+)\s*(characters?|chars?|letters?|digits?)/i);
            if (lengthMatch) {
                constraints.maxLength = parseInt(lengthMatch[1]);
            }
            
            // Unique constraint
            if (sentTokens.some(t => this.constraintKeywords.unique.includes(t))) {
                constraints.unique = true;
            }
        }
        
        return constraints;
    }
    
    /**
     * Extract relationships between entities
     */
    extractRelationships(tokens, sentences) {
        const relationships = [];
        
        // Look for relationship indicators
        const relationshipWords = ['belongs', 'has', 'contains', 'references', 'relates', 'associated', 'linked'];
        
        for (const sentence of sentences) {
            const sentTokens = this.tokenize(this.normalizeText(sentence));
            
            for (let i = 0; i < sentTokens.length - 2; i++) {
                const token = sentTokens[i];
                const verb = sentTokens[i + 1];
                const target = sentTokens[i + 2];
                
                if (relationshipWords.includes(verb) && this.isNoun(token) && this.isNoun(target)) {
                    relationships.push({
                        from: this.singularize(token),
                        to: this.singularize(target),
                        type: verb
                    });
                }
            }
        }
        
        return relationships;
    }
    
    /**
     * Detect the domain of the prompt
     */
    detectDomain(tokens) {
        const domainScores = {};
        
        for (const [domain, keywords] of Object.entries(this.domainPatterns)) {
            domainScores[domain] = 0;
            for (const token of tokens) {
                if (keywords.includes(token)) {
                    domainScores[domain]++;
                }
            }
        }
        
        // Find domain with highest score
        let maxScore = 0;
        let detectedDomain = 'general';
        
        for (const [domain, score] of Object.entries(domainScores)) {
            if (score > maxScore) {
                maxScore = score;
                detectedDomain = domain;
            }
        }
        
        return detectedDomain;
    }
    
    /**
     * Build a schema from extracted information
     */
    buildSchema(entities, fields, constraints, relationships, domain) {
        const schema = {
            type: 'object',
            title: entities[0] || 'Model',
            description: `Generated schema for ${domain} domain`,
            properties: {},
            required: []
        };
        
        // Add default _id field
        schema.properties._id = {
            type: 'string',
            format: 'uuid',
            description: 'Unique identifier'
        };
        
        // Add extracted fields
        for (const field of fields) {
            const fieldSchema = {
                type: field.type === 'email' ? 'string' : field.type,
                description: field.description
            };
            
            // Add format if applicable
            if (field.type === 'string' || field.type === 'email' || field.type === 'url' || field.type === 'date') {
                if (field.type === 'email' || field.name.toLowerCase().includes('email')) {
                    fieldSchema.type = 'string';
                    fieldSchema.format = 'email';
                } else if (field.type === 'url' || field.name.toLowerCase().includes('url')) {
                    fieldSchema.type = 'string';
                    fieldSchema.format = 'uri';
                } else if (field.type === 'date' || field.name.toLowerCase().includes('date')) {
                    fieldSchema.type = 'string';
                    fieldSchema.format = 'date-time';
                } else if (field.type === 'uuid' || field.name.toLowerCase().includes('id')) {
                    fieldSchema.type = 'string';
                    fieldSchema.format = 'uuid';
                }
            }
            
            schema.properties[field.name] = fieldSchema;
            
            // Mark as required if needed
            if (constraints.required) {
                schema.required.push(field.name);
            }
        }
        
        // If no fields were extracted, add default fields based on domain
        if (Object.keys(schema.properties).length === 1) {
            this.addDefaultFields(schema, domain);
        }
        
        // Apply constraints to schema
        if (constraints.minimum !== undefined) {
            schema.minimum = constraints.minimum;
        }
        if (constraints.maximum !== undefined) {
            schema.maximum = constraints.maximum;
        }
        
        return schema;
    }
    
    /**
     * Add default fields based on domain
     */
    addDefaultFields(schema, domain) {
        const domainFields = {
            user: {
                name: { type: 'string' },
                email: { type: 'string', format: 'email' },
                age: { type: 'integer', minimum: 0, maximum: 120 },
                createdAt: { type: 'string', format: 'date-time' }
            },
            product: {
                name: { type: 'string' },
                description: { type: 'string' },
                price: { type: 'number', minimum: 0 },
                quantity: { type: 'integer', minimum: 0 },
                sku: { type: 'string' }
            },
            order: {
                orderNumber: { type: 'string' },
                customerId: { type: 'string' },
                items: { type: 'array', items: { type: 'object' } },
                total: { type: 'number', minimum: 0 },
                status: { type: 'string', enum: ['pending', 'processing', 'shipped', 'delivered'] }
            },
            general: {
                name: { type: 'string' },
                description: { type: 'string' },
                value: { type: 'number' },
                active: { type: 'boolean' },
                createdAt: { type: 'string', format: 'date-time' }
            }
        };
        
        const fields = domainFields[domain] || domainFields.general;
        Object.assign(schema.properties, fields);
        
        // Mark name as required if present
        if (schema.properties.name) {
            schema.required.push('name');
        }
    }
    
    /**
     * Helper: Check if a word is likely a noun
     */
    isNoun(word) {
        if (!word) return false;
        
        // Simple heuristic: words ending in common noun suffixes
        const nounSuffixes = ['er', 'or', 'ist', 'ism', 'ity', 'ment', 'ness', 'tion', 'sion'];
        const hasNounSuffix = nounSuffixes.some(suffix => word.endsWith(suffix));
        
        // Check if it's in our domain patterns
        const isDomainWord = Object.values(this.domainPatterns).flat().includes(word);
        
        // Check if it starts with uppercase (proper noun) - but we normalized to lowercase
        // So we'll just check if it's not a common verb/adjective
        const commonNonNouns = ['is', 'are', 'was', 'were', 'have', 'has', 'had', 'do', 'does', 'did', 
                               'will', 'would', 'should', 'could', 'can', 'may', 'might', 'must',
                               'the', 'a', 'an', 'and', 'or', 'but', 'if', 'when', 'where', 'how'];
        
        return (hasNounSuffix || isDomainWord) && !commonNonNouns.includes(word);
    }
    
    /**
     * Helper: Convert plural to singular
     */
    singularize(word) {
        if (word.endsWith('ies')) {
            return word.slice(0, -3) + 'y';
        } else if (word.endsWith('es')) {
            return word.slice(0, -2);
        } else if (word.endsWith('s') && !word.endsWith('ss')) {
            return word.slice(0, -1);
        }
        return word;
    }
    
    /**
     * Helper: Convert to camelCase
     */
    camelCase(text) {
        const words = text.split(/[\s_-]+/);
        if (words.length === 0) return text;
        
        return words[0].toLowerCase() + 
               words.slice(1).map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join('');
    }
    
    /**
     * Helper: Extract list items (handles commas and 'and')
     */
    extractListItems(tokens) {
        const items = [];
        let current = [];
        
        for (let i = 0; i < tokens.length; i++) {
            const token = tokens[i];
            
            // Check for list separators
            if (token === 'and' || token.includes(',')) {
                // Handle comma attached to word (e.g., "name,")
                const cleanToken = token.replace(/,$/, '');
                if (cleanToken && cleanToken !== 'and') {
                    items.push(cleanToken);
                } else if (current.length > 0) {
                    items.push(current.join(' '));
                    current = [];
                }
            } else {
                // Regular word - might be a simple field name
                items.push(token);
            }
        }
        
        // Add remaining tokens
        if (current.length > 0) {
            items.push(current.join(' '));
        }
        
        return items.filter(item => item.length > 0 && item !== 'and');
    }
    
    /**
     * Helper: Extract number from text
     */
    extractNumber(text) {
        const match = text.match(/\d+/);
        return match ? parseInt(match[0]) : null;
    }
    
    /**
     * Helper: Infer type from field name
     */
    inferTypeFromName(name) {
        const lowerName = name.toLowerCase();
        
        // Check for specific patterns
        if (lowerName.includes('count') || lowerName.includes('num') || 
            lowerName.includes('qty') || lowerName.includes('quantity')) {
            return 'integer';
        }
        
        if (lowerName.includes('price') || lowerName.includes('amount') || 
            lowerName.includes('cost') || lowerName.includes('total')) {
            return 'number';
        }
        
        if (lowerName.includes('is') || lowerName.includes('has') || 
            lowerName.includes('active') || lowerName.includes('enabled')) {
            return 'boolean';
        }
        
        if (lowerName.includes('date') || lowerName.includes('time') || 
            lowerName.includes('created') || lowerName.includes('updated')) {
            return 'date';
        }
        
        if (lowerName.includes('email') || lowerName.includes('mail')) {
            return 'email';
        }
        
        if (lowerName.includes('url') || lowerName.includes('link')) {
            return 'url';
        }
        
        if (lowerName.includes('id') || lowerName.includes('uuid')) {
            return 'uuid';
        }
        
        if (lowerName.includes('list') || lowerName.includes('items') || 
            lowerName.includes('tags')) {
            return 'array';
        }
        
        return 'string';  // Default type
    }
    
    /**
     * Calculate confidence score for the analysis
     */
    calculateConfidence(entities, fields) {
        let score = 0;
        
        // More entities and fields = higher confidence
        if (entities.length > 0) score += 0.3;
        if (fields.length > 0) score += 0.3;
        if (fields.length > 3) score += 0.2;
        if (entities.length > 1) score += 0.2;
        
        return Math.min(score, 1.0);
    }
    
    /**
     * Generate sample documents from a prompt
     */
    generateSamplesFromPrompt(prompt, count = 5) {
        const analysis = this.analyze(prompt);
        const samples = [];
        
        for (let i = 0; i < count; i++) {
            const sample = this.generateSampleFromSchema(analysis.schema);
            samples.push(sample);
        }
        
        return {
            samples,
            schema: analysis.schema,
            metadata: analysis.metadata
        };
    }
    
    /**
     * Generate a sample document from schema
     */
    generateSampleFromSchema(schema) {
        const doc = {};
        
        for (const [key, fieldSchema] of Object.entries(schema.properties || {})) {
            doc[key] = this.generateSampleValue(fieldSchema, key);
        }
        
        return doc;
    }
    
    /**
     * Generate a sample value for a field
     */
    generateSampleValue(fieldSchema, fieldName) {
        const type = fieldSchema.type;
        const format = fieldSchema.format;
        
        switch (type) {
            case 'string':
                if (format === 'email') return `user${Math.floor(Math.random() * 1000)}@example.com`;
                if (format === 'uri') return `https://example.com/path${Math.floor(Math.random() * 100)}`;
                if (format === 'date-time') return new Date().toISOString();
                if (format === 'uuid') return this.generateUUID();
                if (fieldSchema.enum) return fieldSchema.enum[Math.floor(Math.random() * fieldSchema.enum.length)];
                return `${fieldName}_${Math.floor(Math.random() * 1000)}`;
                
            case 'number':
                const min = fieldSchema.minimum || 0;
                const max = fieldSchema.maximum || 1000;
                return Math.random() * (max - min) + min;
                
            case 'integer':
                const intMin = fieldSchema.minimum || 0;
                const intMax = fieldSchema.maximum || 100;
                return Math.floor(Math.random() * (intMax - intMin + 1)) + intMin;
                
            case 'boolean':
                return Math.random() > 0.5;
                
            case 'array':
                const itemCount = Math.floor(Math.random() * 5) + 1;
                const items = [];
                for (let i = 0; i < itemCount; i++) {
                    if (fieldSchema.items) {
                        items.push(this.generateSampleValue(fieldSchema.items, `${fieldName}_item`));
                    } else {
                        items.push(`item_${i}`);
                    }
                }
                return items;
                
            case 'object':
                return this.generateSampleFromSchema(fieldSchema);
                
            default:
                return null;
        }
    }
    
    /**
     * Generate a simple UUID
     */
    generateUUID() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }
}

export default PromptAnalyzer;