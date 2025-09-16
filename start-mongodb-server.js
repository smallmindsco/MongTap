#!/usr/bin/env node

/**
 * Start MongTap MongoDB Server
 * This server implements MongoDB wire protocol with DataFlood backing
 */

import MongoDBServer from './src/welldb-node/server/mongodb-server.js';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const port = process.env.MONGODB_PORT || 27017;
const database = process.env.MONGODB_DATABASE || 'test';
const modelPath = process.env.MONGTAP_MODELS_PATH || join(__dirname, 'mcp-models');

console.log('🚀 Starting MongTap MongoDB Server');
console.log('=====================================');
console.log(`Port: ${port}`);
console.log(`Default Database: ${database}`);
console.log(`Models Path: ${modelPath}`);
console.log('=====================================\n');

const server = new MongoDBServer({
    port: parseInt(port),
    database,
    modelPath,
    logger: {
        debug: console.log,
        info: console.log,
        warn: console.warn,
        error: console.error
    }
});

// Handle shutdown gracefully
process.on('SIGINT', async () => {
    console.log('\n⏹️  Shutting down server...');
    try {
        await server.stop();
        console.log('✅ Server stopped successfully');
        process.exit(0);
    } catch (err) {
        console.error('❌ Error stopping server:', err);
        process.exit(1);
    }
});

process.on('SIGTERM', async () => {
    await server.stop();
    process.exit(0);
});

// Start the server
server.start()
    .then(() => {
        console.log('✅ MongoDB server started successfully!');
        console.log('\n📡 Connection Information:');
        console.log(`   MongoDB URI: mongodb://localhost:${port}/${database}`);
        console.log(`   Host: localhost`);
        console.log(`   Port: ${port}`);
        console.log(`   Database: ${database}`);
        console.log('\n🔗 You can now connect using:');
        console.log('   - MongoDB Compass');
        console.log('   - mongo shell: mongo localhost:' + port + '/' + database);
        console.log('   - Node.js: mongodb://localhost:' + port + '/' + database);
        console.log('\n⌨️  Press Ctrl+C to stop the server\n');
    })
    .catch(err => {
        console.error('❌ Failed to start server:', err);
        process.exit(1);
    });