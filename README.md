# MongTap - MongoDB MCP Server for LLMs

Demo video on YouTube:

[![Watch the video](https://img.youtube.com/vi/Uv4nM2C9qoc/default.jpg)](https://youtu.be/Uv4nM2C9qoc)


**By [SmallMinds LLC Co](https://smallminds.co)**

MongTap is a Model Context Protocol (MCP) server that provides MongoDB-compatible database functionality through statistical modeling. It allows LLMs like Claude to create, query, and manage databases using natural language, without requiring actual data storage.

**Repository**: [github.com/smallmindsco/MongTap](https://github.com/smallmindsco/MongTap)  
**Website**: [smallminds.co](https://smallminds.co)  
**Contact**: andrew@smallminds.co

## Features

- 🚀 **MongoDB Wire Protocol** - Full compatibility with MongoDB drivers and tools
- 🧠 **Statistical Modeling** - Uses DataFlood technology to generate realistic data on-the-fly
- 🔧 **MCP Integration** - Works seamlessly with Claude Desktop and other MCP-compatible LLMs
- 📊 **Natural Language** - Train models from descriptions or sample data
- ⚡ **High Performance** - Generate 20,000+ documents per second
- 🎯 **Zero Storage** - Data is generated statistically, not stored

## Further Documentation

- [Generation Control Parameters](docs/GENERATION_CONTROL.md) - Control document generation with $seed and $entropy
- [MCPB Installation](docs/MCPB_INSTALLATION.md) - Guide for MCPB bundle installation


## Installation

### Prerequisites

- Node.js 20+ 
- Claude Desktop (for MCP integration)
- No MongoDB installation required!

### Quick Start

1. Clone the repository:
```bash
git clone https://github.com/smallmindsco/MongTap.git
cd MongTap
```

2. Install dependencies (minimal):
```bash
npm install
```

3. Test the installation:
```bash
node src/mcp/index.js
```

4. Start MongoDB server (optional):
```bash
node start-mongodb-server.js
```

## Claude Desktop Configuration

To use MongTap with Claude Desktop, you need to configure it as an MCP server.

### 1. Locate Claude Desktop Configuration

Find your Claude Desktop configuration file:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

### 2. Add MongTap to Configuration

Edit the configuration file and add MongTap to the `mcpServers` section:

```json
{
  "mcpServers": {
    "mongtap": {
      "command": "node",
      "args": [
        "/absolute/path/to/MongTap/src/mcp/index.js"
      ],
      "env": {
        "NODE_ENV": "production",
        "LOG_LEVEL": "info"
      }
    }
  }
}
```

**Important**: Replace `/absolute/path/to/MongTap` with the actual path to your MongTap installation.

### 3. Restart Claude Desktop

After saving the configuration, restart Claude Desktop for the changes to take effect.

## Using MongTap in Claude Desktop

Once configured, MongTap provides powerful tools for database operations and data generation.

### Quick Reference

| Tool | Purpose | Key Feature |
|------|---------|-------------|
| `generateDataModel` | Create statistical models | From samples or descriptions |
| `startMongoServer` | Start MongoDB server | Full wire protocol support |
| `stopMongoServer` | Stop server instance | Clean shutdown |
| `listActiveServers` | View running servers | Monitor all instances |
| `queryModel` | Generate documents | $seed and $entropy control |
| `trainModel` | Improve models | Incremental learning |
| `listModels` | View available models | Local model inventory |
| `getModelInfo` | Model details | Schema and statistics |

### MCP Tools Reference

#### 1. generateDataModel
**Description**: Create a statistical model from sample documents or a text description for data generation.

**Parameters**:
- `name` (required): Name for the model
- `description` (optional): Natural language description of the data structure
- `samples` (optional): Array of sample documents to train the model

**Example**:
```javascript
generateDataModel({
  name: "users",
  description: "User profiles with name, email, age, and signup date"
})
// OR with samples
generateDataModel({
  name: "products",
  samples: [
    { name: "Laptop", price: 999, category: "Electronics" },
    { name: "Desk", price: 299, category: "Furniture" }
  ]
})
```

#### 2. startMongoServer
**Description**: Start a local MongoDB-compatible server that generates data from statistical models.

**Parameters**:
- `port` (optional): Port to listen on (0 for auto-assign, default: 27017)
- `database` (optional): Default database name (default: "mcp")

**Example**:
```javascript
startMongoServer({ port: 27017, database: "myapp" })
// Returns: { port: 27017, status: "running" }
```

#### 3. stopMongoServer
**Description**: Stop a running MongoDB-compatible server instance by port number.

**Parameters**:
- `port` (required): Port of the server to stop

**Example**:
```javascript
stopMongoServer({ port: 27017 })
// Returns: { success: true, message: "Server stopped" }
```

#### 4. listActiveServers
**Description**: Get a list of all currently running MongoDB-compatible server instances.

**Parameters**: None

**Example**:
```javascript
listActiveServers()
// Returns: { count: 2, servers: [
//   { port: 27017, database: "test", status: "running", uptime: 3600 },
//   { port: 27018, database: "dev", status: "running", uptime: 1800 }
// ]}
```

#### 5. queryModel
**Description**: Generate documents from a statistical model with optional query filters and generation control.

**Parameters**:
- `model` (required): Name of the model to query
- `query` (optional): MongoDB-style query with special parameters:
  - `$seed`: Number for reproducible generation
  - `$entropy`: Number 0-1 to control randomness level
- `count` (optional): Number of documents to generate (default: 10)

**Example**:
```javascript
queryModel({
  model: "users",
  query: { age: { $gte: 18 }, $seed: 42, $entropy: 0.3 },
  count: 5
})
// Returns 5 consistently generated adult users with low randomness
```

#### 6. trainModel
**Description**: Update an existing statistical model with additional sample documents to improve generation quality.

**Parameters**:
- `model` (required): Name of the model to train
- `documents` (required): Array of documents to train with

**Example**:
```javascript
trainModel({
  model: "products",
  documents: [
    { name: "Mouse", price: 29, category: "Electronics" },
    { name: "Chair", price: 199, category: "Furniture" }
  ]
})
// Returns: { success: true, samplesAdded: 2, totalSamples: 4 }
```

#### 7. listModels
**Description**: Get a list of all available statistical models stored locally.

**Parameters**: None

**Example**:
```javascript
listModels()
// Returns: ["users", "products", "orders", "inventory"]
```

#### 8. getModelInfo
**Description**: Retrieve detailed schema and statistics for a specific statistical model.

**Parameters**:
- `model` (required): Name of the model

**Example**:
```javascript
getModelInfo({ model: "users" })
// Returns: {
//   name: "users",
//   schema: { type: "object", properties: { ... } },
//   sampleCount: 100,
//   lastUpdated: "2025-01-15T10:30:00Z",
//   fields: ["name", "email", "age", "signupDate"]
// }
```

### MCP Prompts

MongTap includes pre-built prompts for common database scenarios:

#### 1. create_ecommerce_db
**Description**: Create a complete e-commerce database with products, customers, and orders.

**Usage**: Ask Claude to "use the create_ecommerce_db prompt" to instantly set up a full e-commerce database structure.

#### 2. create_user_profile
**Description**: Create a user profile model with authentication and preferences.

**Usage**: Perfect for quickly setting up user management systems.

#### 3. analyze_model
**Description**: Analyze an existing model and provide insights about its structure.

**Usage**: Understand the patterns and distributions in your statistical models.

#### 4. generation_control
**Description**: Learn about using $seed and $entropy parameters for controlled generation.

**Usage**: Get detailed information about reproducible and controlled data generation.

### MCP Resources

MongTap provides read-only resources for monitoring:

#### 1. models://list
**Description**: List of all trained DataFlood models  
**Type**: application/json  
**Returns**: Array of model names and metadata

#### 2. servers://status
**Description**: Status of all MongoDB servers  
**Type**: application/json  
**Returns**: Server ports, databases, and connection counts

#### 3. models://{name}/schema
**Description**: Get the JSON schema for a specific model  
**Type**: application/json  
**Returns**: Complete JSON Schema definition

#### 4. models://{name}/sample
**Description**: Get sample data from a model  
**Type**: application/json  
**Returns**: Array of sample documents

#### 5. docs://generation-control
**Description**: Documentation for $seed and $entropy query parameters  
**Type**: text/markdown  
**Returns**: Detailed usage guide

### Example Conversations with Claude

#### Creating a Data Model from Description

```
You: Create a data model for a user profile with name, email, age, and registration date.

Claude: I'll create a user profile model for you using MongTap.

[Uses generateDataModel tool]

I've created a user profile model with the following structure:
- name (string)
- email (string, email format)
- age (integer, 0-120)
- registrationDate (string, date-time format)

The model is now ready to generate realistic user data.
```

#### Starting a MongoDB Server

```
You: Start a MongoDB server for my application on port 27017.

Claude: I'll start a MongoDB server for you.

[Uses startMongoServer tool]

MongoDB server started successfully:
- Port: 27017
- Database: test
- Connection string: mongodb://localhost:27017/test

You can now connect to this server using any MongoDB client or driver.

The server supports advanced generation control through $seed and $entropy parameters in queries.
```

#### Generating Sample Data

```
You: Generate 100 user profiles from the model we created.

Claude: I'll generate 100 user profiles for you.

[Uses queryModel tool]

Generated 100 user profiles. Here's a sample:
1. { name: "John Smith", email: "john.smith@example.com", age: 28, registrationDate: "2024-01-15T10:30:00Z" }
2. { name: "Jane Doe", email: "jane.doe@example.com", age: 34, registrationDate: "2024-02-20T14:45:00Z" }
...

All documents follow the statistical patterns of your model.
```

#### Training from Sample Data

```
You: Here are some product samples. Train a model from them:
- { name: "Widget", price: 19.99, category: "tools", stock: 100 }
- { name: "Gadget", price: 29.99, category: "electronics", stock: 50 }
- { name: "Gizmo", price: 39.99, category: "tools", stock: 75 }

Claude: I'll train a product model from your samples.

[Uses generateDataModel tool with samples]

Model trained successfully! The model learned:
- name: product names (string patterns)
- price: range 19.99-39.99 (number distribution)
- category: "tools" or "electronics" (enum)
- stock: range 50-100 (integer distribution)

The model can now generate similar products with realistic variations.
```

## Connecting with MongoDB Clients

MongTap servers are fully compatible with MongoDB clients. Once you've started a server through Claude:

### Using MongoDB Shell (mongosh)

```bash
mongosh "mongodb://localhost:27017/test"
```

### Using Node.js MongoDB Driver

```javascript
const { MongoClient } = require('mongodb');

async function connect() {
    const client = new MongoClient('mongodb://localhost:27017');
    await client.connect();
    
    const db = client.db('test');
    const collection = db.collection('users');
    
    // Insert triggers model training
    await collection.insertMany([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 }
    ]);
    
    // Find generates data from the model
    const users = await collection.find({}).toArray();
    console.log(users); // Statistically generated users
    
    // Use generation control parameters
    const consistentData = await collection.find({
        $seed: 12345,  // Consistent results
        $entropy: 0.5  // Moderate randomness
    }).toArray();
    
    await client.close();
}
```

### Using Python (pymongo)

```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['test']
collection = db['users']

# Insert trains the model
collection.insert_many([
    {'name': 'Alice', 'age': 30},
    {'name': 'Bob', 'age': 25}
])

# Find generates from the model
for user in collection.find():
    print(user)  # Statistically generated users
    
# Use generation control
for user in collection.find({'$seed': 12345, '$entropy': 0.3}):
    print(user)  # Consistent, low-entropy generation
```

## Advanced Usage

### Multiple Server Instances

You can run multiple MongoDB servers simultaneously:

```
You: Start three MongoDB servers for development, testing, and staging environments.

Claude: I'll start three separate MongoDB servers for you.

[Uses startMongoServer multiple times]

Started 3 MongoDB servers:
1. Development - Port: 27017, Database: dev
2. Testing - Port: 27018, Database: test  
3. Staging - Port: 27019, Database: staging

Each server maintains its own models and can be accessed independently.
```

### Model Persistence

Models are automatically saved and can be reused across sessions:

```
You: List all available models.

Claude: [Uses listModels tool]

Available models:
1. users - User profiles with name, email, age
2. products - Product catalog with pricing
3. orders - Order management system
4. customers - Customer database

You can query any of these models or train new ones.
```

### Constrained Generation

Generate data with specific constraints and control parameters:

```
You: Generate 50 users aged between 25 and 35 with consistent results using seed 12345.

Claude: I'll generate users with those specific constraints and seed control.

[Uses queryModel with constraints and generation parameters]

Generated 50 users with consistent results:
- All aged between 25-35  
- Using seed 12345 for reproducible generation
- $entropy parameter controls randomness level
- Same query will always return identical results
```

## Configuration Options

### Environment Variables

- `LOG_LEVEL` - Logging level (error, warn, info, debug, trace)
- `MONGTAP_PORT` - Default port for MCP server (default: 3000)
- `MONGTAP_STORAGE` - Path for model storage (default: ./welldb-models)
- `MONGTAP_MAX_SERVERS` - Maximum concurrent MongoDB servers (default: 10)

### MCP Server Modes

The MCP server can run in different modes:

```bash
# Standard I/O mode (for Claude Desktop)
node src/mcp/index.js

# TCP mode (for network access)
node src/mcp/index.js tcp --port 3000

# Standalone mode (for testing)
node src/mcp/index.js standalone
```

## Architecture

MongTap consists of three main components:

1. **DataFlood-JS** - Statistical modeling engine that learns from samples
2. **WellDB-Node** - MongoDB wire protocol implementation
3. **MCP Server** - Integration layer for LLM tools

```
┌─────────────────┐     MCP Protocol      ┌──────────────┐
│ Claude Desktop  │ ◄──────────────────► │  MCP Server  │
└─────────────────┘                       └──────┬───────┘
                                                 │
                                                 ▼
┌─────────────────┐     MongoDB Wire     ┌──────────────┐
│ MongoDB Client  │ ◄──────────────────► │ WellDB-Node  │
└─────────────────┘                       └──────┬───────┘
                                                 │
                                                 ▼
                                          ┌──────────────┐
                                          │ DataFlood-JS │
                                          │  (Modeling)  │
                                          └──────────────┘
```
## Troubleshooting

### Claude Desktop doesn't show MongTap tools

1. Check the configuration file path is correct
2. Ensure the path to MongTap is absolute, not relative
3. Restart Claude Desktop completely
4. Check logs: `tail -f ~/Library/Logs/Claude/mcp-*.log` (macOS)

### MongoDB client can't connect

1. Verify the server is running: Use "listActiveServers" in Claude
2. Check the port is not in use: `lsof -i :27017`
3. Ensure firewall allows local connections
4. Try connecting with IP: `mongodb://127.0.0.1:27017`

### Model generation seems incorrect

1. Provide more sample data for better training
2. Use consistent data formats in samples
3. Check model info to see learned patterns
4. Retrain with additional constraints if needed

## Development

### Running Tests

```bash
# Run all tests
npm test

# Run specific test suite
node test/mcp/test-mcp-server.js
node test/welldb-node/test-mongodb-server.js
node test/dataflood-js/test-inferrer.js

# Run integration tests
node test/welldb-node/test-integration.js
```

### Project Structure

```
MongTap/
├── src/
│   ├── mcp/                 # MCP server implementation
│   │   ├── mcp-server.js    # Core MCP server
│   │   ├── prompt-analyzer.js # NLP for prompts
│   │   └── server-manager.js # Multi-server management
│   ├── welldb-node/         # MongoDB protocol
│   │   ├── server/          # MongoDB server implementation
│   │   └── storage/         # DataFlood storage adapter
│   └── dataflood-js/        # Statistical modeling
│       ├── schema/          # Schema inference
│       ├── generator/       # Document generation
│       └── training/        # Model training
└── README.md               # This file
```

## License

MIT License - See [LICENSE](LICENSE) file for details.

## Privacy Policy

MongTap is designed with privacy as a fundamental principle:

### Data Collection
- **NO personal data collection** - MongTap does not collect any user data
- **NO analytics or tracking** - No usage statistics are gathered
- **NO external connections** - All operations are performed locally
- **NO data persistence** - Models are statistical representations, not actual data

### Data Storage
- All models are stored locally on your machine
- Storage locations are fully configurable via `mongtap.config.json`
- No cloud services or external storage is used
- Generated data is synthetic and does not represent real information

### Data Security
- Local-only operation ensures data never leaves your machine
- No authentication required (no credentials to compromise)
- Open source code allows full security auditing
- Input validation prevents injection attacks

## Security

MongTap implements comprehensive security measures:

- **Input Validation**: All inputs are validated before processing
- **Error Handling**: Graceful error handling prevents information leakage
- **No External Dependencies**: Core functionality has minimal dependencies
- **Local Operation**: No network exposure unless explicitly configured
- **Open Source**: Full code transparency for security auditing

For detailed security information, see [docs/SECURITY_AUDIT.md](docs/SECURITY_AUDIT.md).

## Support

- **Issues**: [GitHub Issues](https://github.com/smallmindsco/MongTap/issues)
- **Documentation**: [docs/](docs/)
- **Status**: [DesignDocuments/STATUS.claude](DesignDocuments/STATUS.claude)
- **Contact**: SmallMinds LLC Co - [smallminds.co](https://smallminds.co) - andrew@smallminds.co

## Acknowledgments

- DataFlood technology for statistical modeling
- MongoDB for protocol specification
- Anthropic for MCP protocol
- Claude Desktop for LLM integration

---

**Note**: MongTap generates data statistically and does not store actual data. It's perfect for development, testing, and demonstration purposes where you need realistic data without the overhead of actual storage.
