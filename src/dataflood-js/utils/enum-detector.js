/**
 * Enum Detection Module - Port of DataFlood's EnumDetector.cs
 * Detects whether a set of values should be treated as an enum
 * based on statistical analysis and heuristics
 */

class EnumDetector {
    constructor(config = {}) {
        this.config = {
            largeDatasetSampleSize: config.largeDatasetSampleSize || 10000,
            enableSampling: config.enableSampling !== false,
            useStandardSetMatching: config.useStandardSetMatching !== false,
            useSemanticAnalysis: config.useSemanticAnalysis !== false,
            usePowerLawAnalysis: config.usePowerLawAnalysis !== false,
            minimumConfidence: config.minimumConfidence || 'medium'
        };
        
        this.standardSets = this.initializeStandardSets();
    }
    
    /**
     * Main detection method
     * @param {Array} values - Array of values to analyze
     * @param {string} fieldName - Name of the field (for context)
     * @returns {Object} EnumDetectionResult
     */
    detectEnum(values, fieldName = '') {
        if (!values || values.length === 0) {
            return this.createResult(false, [], 'veryLow', 'No values to analyze', 'notEnum');
        }
        
        // Sample large datasets if needed
        const sampledValues = this.sampleDataset(values);
        
        // Calculate metrics
        const metrics = this.calculateMetrics(sampledValues);
        
        // Apply detection rules based on dataset size
        return this.applyDetectionRules(metrics, fieldName);
    }
    
    /**
     * Sample dataset for performance with large datasets
     */
    sampleDataset(values) {
        if (!this.config.enableSampling || values.length <= this.config.largeDatasetSampleSize) {
            return values;
        }
        
        // Reservoir sampling for uniform random sample
        const sampleSize = this.config.largeDatasetSampleSize;
        const sampled = values.slice(0, sampleSize);
        
        for (let i = sampleSize; i < values.length; i++) {
            const j = Math.floor(Math.random() * (i + 1));
            if (j < sampleSize) {
                sampled[j] = values[i];
            }
        }
        
        return sampled;
    }
    
    /**
     * Calculate statistical metrics for enum detection
     */
    calculateMetrics(values) {
        const metrics = {
            totalCount: values.length,
            uniqueCount: 0,
            uniquenessRatio: 0,
            frequencyDistribution: {},
            averageLength: 0,
            maxLength: 0,
            minLength: Number.MAX_VALUE,
            lengthVariance: 0,
            entropyScore: 0,
            powerLawExponent: 0,
            concentrationRatio: 0,
            semanticCoherence: 0,
            hasCommonPatterns: {
                isStandardSet: false,
                isCodePattern: false,
                isNaturalLanguage: false,
                hasStructuredFormat: false
            }
        };
        
        // Build frequency distribution
        const uniqueValues = new Set();
        let totalLength = 0;
        const lengths = [];
        
        for (const value of values) {
            const strValue = String(value);
            uniqueValues.add(strValue);
            
            metrics.frequencyDistribution[strValue] = 
                (metrics.frequencyDistribution[strValue] || 0) + 1;
            
            const length = strValue.length;
            totalLength += length;
            lengths.push(length);
            metrics.maxLength = Math.max(metrics.maxLength, length);
            metrics.minLength = Math.min(metrics.minLength, length);
        }
        
        metrics.uniqueCount = uniqueValues.size;
        metrics.uniquenessRatio = metrics.uniqueCount / metrics.totalCount;
        metrics.averageLength = totalLength / metrics.totalCount;
        
        // Calculate length variance
        const avgLength = metrics.averageLength;
        const variance = lengths.reduce((sum, len) => 
            sum + Math.pow(len - avgLength, 2), 0) / lengths.length;
        metrics.lengthVariance = Math.sqrt(variance);
        
        // Calculate entropy (Shannon's entropy)
        metrics.entropyScore = this.calculateEntropy(metrics.frequencyDistribution, metrics.totalCount);
        
        // Calculate power law exponent (simplified)
        if (this.config.usePowerLawAnalysis) {
            metrics.powerLawExponent = this.calculatePowerLawExponent(metrics.frequencyDistribution);
        }
        
        // Calculate concentration ratio (top 20% of values)
        metrics.concentrationRatio = this.calculateConcentrationRatio(metrics.frequencyDistribution);
        
        // Check for common patterns
        metrics.hasCommonPatterns = this.detectPatterns(uniqueValues);
        
        // Calculate semantic coherence (simplified)
        if (this.config.useSemanticAnalysis) {
            metrics.semanticCoherence = this.calculateSemanticCoherence(uniqueValues);
        }
        
        return metrics;
    }
    
    /**
     * Calculate Shannon's entropy
     */
    calculateEntropy(distribution, total) {
        let entropy = 0;
        
        for (const count of Object.values(distribution)) {
            if (count > 0) {
                const probability = count / total;
                entropy -= probability * Math.log2(probability);
            }
        }
        
        return entropy;
    }
    
    /**
     * Calculate power law exponent (simplified Zipf's law check)
     */
    calculatePowerLawExponent(distribution) {
        const frequencies = Object.values(distribution).sort((a, b) => b - a);
        
        if (frequencies.length < 10) {
            return 0;
        }
        
        // Simple linear regression in log-log space
        let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        const n = Math.min(frequencies.length, 100); // Limit for performance
        
        for (let i = 0; i < n; i++) {
            const x = Math.log(i + 1);
            const y = Math.log(frequencies[i]);
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumX2 += x * x;
        }
        
        const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        return Math.abs(slope);
    }
    
    /**
     * Calculate concentration ratio (how concentrated the values are)
     */
    calculateConcentrationRatio(distribution) {
        const frequencies = Object.values(distribution).sort((a, b) => b - a);
        const total = frequencies.reduce((sum, freq) => sum + freq, 0);
        
        if (frequencies.length === 0) return 0;
        
        // Calculate what percentage of unique values account for 80% of occurrences
        let cumulativeFreq = 0;
        let topCount = 0;
        
        for (const freq of frequencies) {
            cumulativeFreq += freq;
            topCount++;
            if (cumulativeFreq >= total * 0.8) {
                break;
            }
        }
        
        return topCount / frequencies.length;
    }
    
    /**
     * Detect common patterns in values
     */
    detectPatterns(uniqueValues) {
        const patterns = {
            isStandardSet: false,
            isCodePattern: false,
            isNaturalLanguage: false,
            hasStructuredFormat: false
        };
        
        const valuesArray = Array.from(uniqueValues);
        
        // Check for standard sets
        if (this.config.useStandardSetMatching) {
            patterns.isStandardSet = this.isStandardSet(valuesArray);
        }
        
        // Check for code patterns (e.g., STATUS_OK, ERROR_404)
        patterns.isCodePattern = this.hasCodePattern(valuesArray);
        
        // Check for natural language (simple check)
        patterns.isNaturalLanguage = this.hasNaturalLanguagePattern(valuesArray);
        
        // Check for structured format (e.g., all same format)
        patterns.hasStructuredFormat = this.hasStructuredFormat(valuesArray);
        
        return patterns;
    }
    
    /**
     * Check if values match a standard set
     */
    isStandardSet(values) {
        for (const [setName, standardValues] of Object.entries(this.standardSets)) {
            const overlap = values.filter(v => 
                standardValues.some(sv => sv.toLowerCase() === v.toLowerCase())
            );
            
            // If > 50% overlap with a standard set, consider it a match
            if (overlap.length / values.length > 0.5) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Check for code-like patterns
     */
    hasCodePattern(values) {
        const codePatterns = [
            /^[A-Z_]+$/,           // CONSTANT_CASE
            /^[A-Z][A-Z0-9_]*$/,   // UPPER_SNAKE_CASE
            /^\w+_\w+$/,           // snake_case
            /^[A-Z][a-z]+(?:[A-Z][a-z]+)*$/, // PascalCase
        ];
        
        let matchCount = 0;
        for (const value of values) {
            if (codePatterns.some(pattern => pattern.test(value))) {
                matchCount++;
            }
        }
        
        return matchCount / values.length > 0.7;
    }
    
    /**
     * Check for natural language patterns
     */
    hasNaturalLanguagePattern(values) {
        let naturalCount = 0;
        
        for (const value of values) {
            // Simple check: contains spaces and mostly letters
            if (/^[A-Za-z\s,.'"-]+$/.test(value) && value.includes(' ')) {
                naturalCount++;
            }
        }
        
        return naturalCount / values.length > 0.5;
    }
    
    /**
     * Check if values follow a structured format
     */
    hasStructuredFormat(values) {
        if (values.length < 3) return false;
        
        // Check for consistent patterns like XXX-000, ABC123, etc.
        const formatPatterns = values.map(v => 
            String(v).replace(/[A-Z]/g, 'A')
             .replace(/[a-z]/g, 'a')
             .replace(/[0-9]/g, '0')
             .replace(/[^Aa0]/g, 'X')
        );
        
        const uniquePatterns = new Set(formatPatterns);
        
        // If most values follow the same 1-3 patterns, it's structured
        // Fixed logic: check if we have few unique patterns relative to total values
        const patternRatio = uniquePatterns.size / values.length;
        return uniquePatterns.size <= 3 || patternRatio < 0.2;
    }
    
    /**
     * Calculate semantic coherence (simplified)
     */
    calculateSemanticCoherence(uniqueValues) {
        const values = Array.from(uniqueValues);
        
        if (values.length < 2) return 0;
        
        // Simple coherence: check if values share common prefixes/suffixes
        let commonPrefixScore = 0;
        let commonSuffixScore = 0;
        
        // Find common prefix length
        const minLength = Math.min(...values.map(v => v.length));
        for (let i = 0; i < minLength; i++) {
            const char = values[0][i];
            if (values.every(v => v[i] === char)) {
                commonPrefixScore++;
            } else {
                break;
            }
        }
        
        // Find common suffix length
        for (let i = 0; i < minLength; i++) {
            const char = values[0][values[0].length - 1 - i];
            if (values.every(v => v[v.length - 1 - i] === char)) {
                commonSuffixScore++;
            } else {
                break;
            }
        }
        
        // Normalize scores
        const maxScore = Math.max(commonPrefixScore, commonSuffixScore);
        return minLength > 0 ? maxScore / minLength : 0;
    }
    
    /**
     * Apply detection rules based on dataset size
     */
    applyDetectionRules(metrics, fieldName) {
        const datasetSize = metrics.totalCount;
        
        if (datasetSize <= 1000) {
            return this.detectSmallDatasetEnum(metrics, fieldName);
        } else if (datasetSize <= 100000) {
            return this.detectMediumDatasetEnum(metrics, fieldName);
        } else {
            return this.detectLargeDatasetEnum(metrics, fieldName);
        }
    }
    
    /**
     * Detection rules for small datasets (≤1000 items)
     */
    detectSmallDatasetEnum(metrics, fieldName) {
        // Very high cardinality suggests not an enum
        if (metrics.uniquenessRatio > 0.8) {
            return this.createResult(false, [], 'veryLow', 
                'Too many unique values for dataset size', 'notEnum');
        }
        
        // Standard sets are always enums
        if (metrics.hasCommonPatterns.isStandardSet) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution), 
                'high', 'Matches standard set', 'standardSet');
        }
        
        // Code patterns with reasonable cardinality
        if (metrics.hasCommonPatterns.isCodePattern && metrics.uniqueCount <= 50) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'high', 'Code pattern with reasonable cardinality', 'applicationEnum');
        }
        
        // Low cardinality with high concentration
        if (metrics.uniqueCount <= 20 && metrics.concentrationRatio < 0.3) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'medium', 'Low cardinality with high concentration', 'applicationEnum');
        }
        
        // Natural language patterns usually aren't enums
        if (metrics.hasCommonPatterns.isNaturalLanguage) {
            return this.createResult(false, [], 'low',
                'Natural language pattern detected', 'notEnum');
        }
        
        // Borderline cases
        if (metrics.uniqueCount <= 30 && metrics.entropyScore < 3) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'low', 'Borderline case - low entropy', 'applicationEnum');
        }
        
        return this.createResult(false, [], 'veryLow',
            'Does not meet enum criteria', 'notEnum');
    }
    
    /**
     * Detection rules for medium datasets (≤100,000 items)
     */
    detectMediumDatasetEnum(metrics, fieldName) {
        // Very high cardinality definitely not an enum
        if (metrics.uniquenessRatio > 0.5 || metrics.uniqueCount > 1000) {
            return this.createResult(false, [], 'veryLow',
                'Cardinality too high for enum', 'notEnum');
        }
        
        // Standard sets
        if (metrics.hasCommonPatterns.isStandardSet) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'high', 'Matches standard set', 'standardSet');
        }
        
        // Strong power law suggests enum-like behavior
        if (metrics.powerLawExponent > 1.5 && metrics.uniqueCount <= 100) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'medium', 'Strong power law distribution', 'applicationEnum');
        }
        
        // Code patterns with reasonable cardinality
        if (metrics.hasCommonPatterns.isCodePattern && metrics.uniqueCount <= 200) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'medium', 'Code pattern detected', 'applicationEnum');
        }
        
        // Very low entropy with reasonable cardinality
        if (metrics.entropyScore < 4 && metrics.uniqueCount <= 50) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'medium', 'Low entropy categorical data', 'applicationEnum');
        }
        
        return this.createResult(false, [], 'low',
            'Does not meet enum criteria for medium dataset', 'notEnum');
    }
    
    /**
     * Detection rules for large datasets (>100,000 items)
     */
    detectLargeDatasetEnum(metrics, fieldName) {
        // For large datasets, be very conservative
        if (metrics.uniqueCount > 500) {
            return this.createResult(false, [], 'veryLow',
                'Too many unique values for large dataset', 'notEnum');
        }
        
        // Standard sets with high confidence
        if (metrics.hasCommonPatterns.isStandardSet && metrics.semanticCoherence > 0.3) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'high', 'Standard set with high coherence', 'standardSet');
        }
        
        // Very strong power law with low cardinality
        if (metrics.powerLawExponent > 2.0 && metrics.uniqueCount <= 50) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'medium', 'Strong categorical behavior in large dataset', 'applicationEnum');
        }
        
        // Structured format with very low cardinality
        if (metrics.hasCommonPatterns.hasStructuredFormat && metrics.uniqueCount <= 20) {
            return this.createResult(true, Object.keys(metrics.frequencyDistribution),
                'low', 'Structured categorical data', 'naturalConstraint');
        }
        
        return this.createResult(false, [], 'veryLow',
            'Large dataset does not exhibit enum characteristics', 'notEnum');
    }
    
    /**
     * Create result object
     */
    createResult(shouldCreate, enumValues, confidence, reasoning, category) {
        return {
            shouldCreateEnum: shouldCreate,
            enumValues: enumValues.slice(0, 1000), // Limit for performance
            confidence: confidence,
            reasoning: reasoning,
            category: category
        };
    }
    
    /**
     * Initialize standard sets for matching
     */
    initializeStandardSets() {
        return {
            booleans: ['true', 'false', 'yes', 'no', '1', '0', 'on', 'off'],
            months: ['January', 'February', 'March', 'April', 'May', 'June',
                    'July', 'August', 'September', 'October', 'November', 'December',
                    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            weekdays: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday',
                      'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            countries: ['United States', 'Canada', 'Mexico', 'United Kingdom', 'France', 'Germany',
                       'Spain', 'Italy', 'China', 'Japan', 'India', 'Brazil', 'Australia',
                       'USA', 'US', 'UK', 'GB', 'FR', 'DE', 'ES', 'IT', 'CN', 'JP', 'IN', 'BR', 'AU'],
            states: ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
                    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA'],
            currencies: ['USD', 'EUR', 'GBP', 'JPY', 'CNY', 'AUD', 'CAD', 'CHF', 'HKD', 'SGD'],
            httpMethods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'],
            httpStatus: ['200', '201', '204', '301', '302', '400', '401', '403', '404', '500', '502', '503'],
            priorities: ['low', 'medium', 'high', 'critical', 'urgent', '1', '2', '3', '4', '5'],
            sizes: ['small', 'medium', 'large', 'extra-large', 'XS', 'S', 'M', 'L', 'XL', 'XXL'],
            genders: ['male', 'female', 'other', 'M', 'F', 'O'],
            directions: ['north', 'south', 'east', 'west', 'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW']
        };
    }
}

export { EnumDetector };