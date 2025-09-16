# MongTap MCPB Installation Guide

## Quick Installation (Recommended)

### Installing via Claude Desktop

1. **Download the MCPB file**:
   - Download `MongTap.mcpb` from the [latest release](https://github.com/smallmindsllc/mongtap/releases)

2. **Install in Claude Desktop**:
   - Open Claude Desktop
   - Go to Settings → Developer → MCP Servers
   - Click "Install from MCPB" 
   - Select the downloaded `MongTap.mcpb` file
   - Claude Desktop will automatically install and configure MongTap

3. **Verify Installation**:
   - Restart Claude Desktop
   - Type "What MCP servers are available?" to see MongTap listed
   - Try a command like "Generate a customer data model" to test

## Manual Installation

If you prefer manual installation or need to customize the configuration:

1. **Extract the MCPB**:
   ```bash
   # MCPB files are zip archives
   unzip MongTap.mcpb -d ~/mongtap
   cd ~/mongtap
   ```

2. **Install dependencies** (if not bundled):
   ```bash
   npm install
   ```

3. **Configure Claude Desktop**:
   Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:
   ```json
   {
     "mcpServers": {
       "mongtap": {
         "command": "node",
         "args": ["~/mongtap/src/mcp/index.js"]
       }
     }
   }
   ```

4. **Restart Claude Desktop** to load the new configuration

## Using MongTap in Claude

Once installed, you can use MongTap's capabilities:

### Generate Data Models
```
"Generate a data model for an e-commerce product catalog"
"Create a user profile schema with common fields"
```

### Start MongoDB Servers
```
"Start a MongoDB server with the product catalog model"
"Launch a test database on port 27018"
```

### Query and Manage Models
```
"List all available data models"
"Show me information about the customer model"
"Train the product model with this sample data: [...]"
```

## Configuration Options

MongTap can be configured via environment variables:

- `MONGTAP_MODELS_PATH`: Directory for storing models (default: `./mcp-models`)
- `MONGTAP_LOG_LEVEL`: Logging level (error, warn, info, debug, trace)
- `MONGTAP_PORT_START`: Starting port for MongoDB servers (default: 27017)
- `MONGTAP_PORT_END`: Ending port for MongoDB servers (default: 27100)

## Troubleshooting

### MongTap not appearing in Claude Desktop

1. Check the configuration file syntax:
   ```bash
   cat ~/Library/Application\ Support/Claude/claude_desktop_config.json | jq .
   ```

2. Verify the installation path:
   ```bash
   ls -la ~/mongtap/src/mcp/index.js
   ```

3. Check logs:
   ```bash
   tail -f ~/mongtap/logs/mongtap-*.log
   ```

### Connection Issues

1. Ensure no other services are using MongoDB ports (27017-27100)
2. Check firewall settings allow local connections
3. Verify Node.js version is >=20.0.0:
   ```bash
   node --version
   ```

### Performance Issues

1. Increase cache size:
   ```bash
   export MONGTAP_CACHE_SIZE_MB=200
   ```

2. Adjust log level to reduce I/O:
   ```bash
   export MONGTAP_LOG_LEVEL=error
   ```

## Uninstalling

### Via Claude Desktop
1. Go to Settings → Developer → MCP Servers
2. Click the remove button next to MongTap
3. Restart Claude Desktop

### Manual Uninstall
1. Remove from configuration:
   ```bash
   # Edit claude_desktop_config.json and remove the mongtap entry
   ```

2. Delete installation directory:
   ```bash
   rm -rf ~/mongtap
   ```

3. Clean up models (optional):
   ```bash
   rm -rf ~/mcp-models
   ```

## Support

- **Issues**: [GitHub Issues](https://github.com/smallmindsco/MongTap/issues)
- **Documentation**: [MongTap Docs](https://github.com/smallmindsco/MongTap/blob/main/README.md)
- **Email**: andrew@smallminds.co

## License

MongTap is licensed under the MIT License. See LICENSE file for details.
