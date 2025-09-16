# MongTap Generation Control Parameters

## Overview

MongTap extends MongoDB query syntax with special parameters that control how DataFlood generates synthetic documents. These parameters provide fine-grained control over randomness and reproducibility without affecting query filtering logic.

## Quick Start

```javascript
// Basic usage with seed for reproducibility
db.collection('stocks').find({ 
    $seed: 12345,
    price: { $gt: 100 }
}).limit(10)

// Control randomness with entropy
db.collection('users').find({
    $entropy: 0.2,  // Low variation
    age: { $gte: 18 }
})
```

## Parameters

### `$seed` (or `_seed`)

Controls the random number generator seed for deterministic generation.

- **Type**: Number (integer)
- **Purpose**: Ensures reproducible document generation
- **Behavior**: Same seed always generates identical documents
- **Use Cases**:
  - Unit testing with consistent data
  - Debugging data generation issues
  - Creating reproducible demos
  - Benchmarking with consistent datasets

**Example:**
```javascript
// These two queries will return identical documents
const result1 = await db.collection('products').find({ $seed: 42 }).toArray();
const result2 = await db.collection('products').find({ $seed: 42 }).toArray();
// result1 deep equals result2
```

### `$entropy` (or `_entropy`)

Controls the level of randomness/variation in generated documents.

- **Type**: Number (0.0 to 1.0)
- **Purpose**: Adjusts variation in generated data
- **Values**:
  - `0.0` = Minimal variation (highly predictable, centered values)
  - `0.5` = Moderate variation (balanced randomness)
  - `1.0` = Maximum variation (highly random, full range)
- **Default**: Model-specific, typically ~0.5
- **Use Cases**:
  - Low entropy for consistent test data
  - High entropy for stress testing edge cases
  - Moderate entropy for realistic data simulation

**Example:**
```javascript
// Low entropy - predictable, centered values
await db.collection('metrics').find({ 
    $entropy: 0.1,
    type: 'cpu_usage'
}).limit(100)  // Values cluster around mean

// High entropy - wide variation
await db.collection('metrics').find({ 
    $entropy: 0.9,
    type: 'cpu_usage'
}).limit(100)  // Values span full range
```

## Combined Usage

Generation parameters work seamlessly with standard MongoDB query operators:

```javascript
// Complex query with generation control
const results = await db.collection('orders').find({
    // Generation control
    $seed: 999,
    $entropy: 0.3,
    
    // Standard MongoDB query
    status: 'completed',
    total: { $gte: 100, $lte: 1000 },
    createdAt: { $gte: new Date('2024-01-01') },
    'customer.vip': true
}).sort({ total: -1 }).limit(50).toArray();
```

## MongoDB Compatibility

### Supported Query Operations

Generation parameters are compatible with all MongoDB query features:

- **Comparison Operators**: `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`
- **Logical Operators**: `$and`, `$or`, `$not`, `$nor`
- **Element Operators**: `$exists`, `$type`
- **Array Operators**: `$in`, `$nin`, `$all`, `$elemMatch`
- **Evaluation Operators**: `$regex`, `$text`, `$where`
- **Cursor Methods**: `.sort()`, `.limit()`, `.skip()`, `.project()`

### Aggregation Pipeline

Generation parameters can be used in aggregation pipelines:

```javascript
db.collection('sales').aggregate([
    { $match: { 
        $seed: 123,
        region: 'North America' 
    }},
    { $group: {
        _id: '$product',
        totalSales: { $sum: '$amount' }
    }},
    { $sort: { totalSales: -1 }}
])
```

## Implementation Details

### Architecture

1. **Query Processing Flow**:
   ```
   User Query → Extract Generation Params → Remove Params from Query → 
   Generate Documents → Apply Filters → Return Results
   ```

2. **Key Components**:
   - `CollectionManager.extractGenerationParams()` - Extracts `$seed` and `$entropy`
   - `CollectionManager.removeGenerationParams()` - Cleans query for filtering
   - `DocumentGenerator(seed, entropyOverride)` - Uses params for generation
   - `DataFloodStorage.generateDocuments()` - Passes params to generator

### Parameter Isolation

Generation parameters are isolated from query filtering:

```javascript
// This query:
{ $seed: 42, price: { $gt: 100 } }

// Becomes:
generationParams = { seed: 42 }
filterQuery = { price: { $gt: 100 } }
```

Documents are generated with `generationParams`, then filtered with `filterQuery`.

## Use Cases

### 1. Unit Testing

```javascript
describe('Order Processing', () => {
    it('should process high-value orders', async () => {
        const orders = await db.collection('orders').find({
            $seed: 12345,  // Consistent test data
            $entropy: 0.2,  // Low variation
            total: { $gte: 1000 }
        }).toArray();
        
        const results = await processOrders(orders);
        expect(results).toMatchSnapshot();  // Deterministic
    });
});
```

### 2. Performance Benchmarking

```javascript
// Benchmark with consistent data
const configs = [
    { $seed: 1, $entropy: 0.5 },  // Baseline
    { $seed: 1, $entropy: 0.1 },  // Low variation
    { $seed: 1, $entropy: 0.9 }   // High variation
];

for (const config of configs) {
    const start = Date.now();
    await db.collection('records').find({
        ...config,
        status: 'active'
    }).limit(10000).toArray();
    console.log(`Entropy ${config.$entropy}: ${Date.now() - start}ms`);
}
```

### 3. Data Science & Analytics

```javascript
// Generate datasets with controlled characteristics
async function generateDataset(variation = 'medium') {
    const entropyMap = {
        low: 0.1,
        medium: 0.5,
        high: 0.9
    };
    
    return await db.collection('measurements').find({
        $seed: 42,  // Reproducible
        $entropy: entropyMap[variation],
        sensor: 'temperature'
    }).limit(1000).toArray();
}

// Analyze different variation levels
const lowVar = await generateDataset('low');
const highVar = await generateDataset('high');
```

### 4. Demo & Presentation

```javascript
// Consistent demo data for presentations
async function getDemoData() {
    return {
        users: await db.collection('users').find({
            $seed: 2024,
            $entropy: 0.3,
            active: true
        }).limit(5).toArray(),
        
        orders: await db.collection('orders').find({
            $seed: 2024,
            $entropy: 0.3,
            status: 'completed'
        }).limit(10).toArray()
    };
}
```

## Best Practices

### 1. Use Seeds for Testing

Always use seeds in test environments for reproducibility:

```javascript
const TEST_SEED = process.env.TEST_SEED || 12345;

beforeEach(async () => {
    testData = await db.collection('test').find({
        $seed: TEST_SEED
    }).toArray();
});
```

### 2. Adjust Entropy by Use Case

- **Testing**: Low entropy (0.1-0.3) for predictability
- **Development**: Medium entropy (0.4-0.6) for realism
- **Stress Testing**: High entropy (0.7-1.0) for edge cases

### 3. Document Seed Values

When using seeds, document them for team reference:

```javascript
// Seeds used in this project:
const SEEDS = {
    UNIT_TESTS: 12345,
    INTEGRATION_TESTS: 67890,
    DEMO_DATA: 2024,
    BENCHMARK: 1000
};
```

### 4. Combine for Complex Scenarios

```javascript
// Predictable structure with varied values
const data = await db.collection('sensors').find({
    $seed: 100,      // Consistent structure
    $entropy: 0.8,   // Varied readings
    type: 'environmental'
}).toArray();
```

## Troubleshooting

### Issue: Different results with same seed

**Cause**: Query parameters might have changed.

**Solution**: Ensure entire query is identical:
```javascript
// Store complete query
const query = { $seed: 42, category: 'electronics' };
const options = { limit: 10, sort: { price: 1 } };

// Reuse exact same query and options
const result1 = await collection.find(query, options).toArray();
const result2 = await collection.find(query, options).toArray();
```

### Issue: Entropy not affecting variation

**Cause**: Model might have limited variation in training data.

**Solution**: Check model's histogram ranges:
```javascript
const model = await storage.getModel('mcp', 'collection');
console.log(model.properties.field.histogram);  // Check value distribution
```

### Issue: Parameters appearing in results

**Cause**: Generation parameters shouldn't appear in documents.

**Solution**: Verify CollectionManager.removeGenerationParams() is working:
```javascript
// Parameters are stripped before filtering
const docs = await collection.find({ $seed: 42 }).toArray();
console.log(docs[0].$seed);  // Should be undefined
```

## API Reference

### Query Format

```typescript
interface GenerationQuery extends MongoQuery {
    $seed?: number;      // Random seed (integer)
    $entropy?: number;   // Variation level (0.0-1.0)
    _seed?: number;      // Alternative syntax
    _entropy?: number;   // Alternative syntax
    [key: string]: any;  // Standard MongoDB query
}
```

### Internal Methods

```javascript
// CollectionManager methods
extractGenerationParams(query: object): { seed?: number, entropy?: number }
removeGenerationParams(query: object): object

// DocumentGenerator constructor
new DocumentGenerator(seed?: number, entropyOverride?: number)
```

## MCP Integration

The generation control feature is exposed through the Model Context Protocol:

### Tool: `queryModel`
```json
{
    "name": "queryModel",
    "description": "Query a DataFlood model directly. Supports generation control via $seed and $entropy parameters.",
    "inputSchema": {
        "properties": {
            "query": {
                "description": "MongoDB-style query. Special parameters: $seed (number) for reproducible generation, $entropy (0-1) to control randomness level"
            }
        }
    }
}
```

### Prompt: `generation_control`
Access via MCP to learn about generation control parameters.

### Resource: `docs://generation-control`
Full documentation available as MCP resource.

## Version History

- **v1.0.0** - Initial implementation
  - Added `$seed` parameter for reproducible generation
  - Added `$entropy` parameter for variation control
  - Support for both `$` and `_` prefixes
  - Full MongoDB query compatibility

## See Also

- [DataFlood Documentation](https://github.com/YourOrg/DataFlood)
- [MongoDB Query Documentation](https://docs.mongodb.com/manual/tutorial/query-documents/)
- [MongTap README](./README.md)