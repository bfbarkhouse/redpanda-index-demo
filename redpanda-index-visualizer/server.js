require('dotenv').config();

const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { Kafka, CompressionTypes, CompressionCodecs } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const SnappyCodec = require('kafkajs-snappy');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

// Register Snappy compression codec
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

app.use(cors());
app.use(express.static(path.join(__dirname)));

// Kafka configuration with TLS and SASL support
function createKafkaConfig() {
    const brokers = process.env.KAFKA_BROKERS ? 
        process.env.KAFKA_BROKERS.split(',') : 
        ['localhost:9092'];
    
    const config = {
        clientId: process.env.KAFKA_CLIENT_ID || 'redpanda-visualizer',
        brokers: brokers,
        retry: {
            initialRetryTime: 100,
            retries: 8
        }
    };
    
    // Add SSL/TLS configuration if enabled
    if (process.env.KAFKA_SSL === 'true') {
        config.ssl = {
            rejectUnauthorized: process.env.KAFKA_SSL_REJECT_UNAUTHORIZED !== 'false'
        };
        
        // Add custom CA certificate if provided
        if (process.env.KAFKA_SSL_CA_PATH) {
            try {
                config.ssl.ca = [fs.readFileSync(process.env.KAFKA_SSL_CA_PATH, 'utf-8')];
                console.log('Loaded custom CA certificate');
            } catch (error) {
                console.error('Error loading CA certificate:', error.message);
            }
        }
        
        // Add client certificate and key if provided
        if (process.env.KAFKA_SSL_CERT_PATH && process.env.KAFKA_SSL_KEY_PATH) {
            try {
                config.ssl.cert = fs.readFileSync(process.env.KAFKA_SSL_CERT_PATH, 'utf-8');
                config.ssl.key = fs.readFileSync(process.env.KAFKA_SSL_KEY_PATH, 'utf-8');
                console.log('Loaded client certificate and key');
            } catch (error) {
                console.error('Error loading client certificate/key:', error.message);
            }
        }
        
        console.log('SSL/TLS enabled for Kafka connection');
    }
    
    // Add SASL authentication if enabled
    if (process.env.KAFKA_SASL_MECHANISM) {
        const mechanism = process.env.KAFKA_SASL_MECHANISM.toLowerCase();
        
        config.sasl = {
            mechanism: mechanism
        };
        
        switch (mechanism) {
            case 'plain':
                config.sasl.username = process.env.KAFKA_SASL_USERNAME;
                config.sasl.password = process.env.KAFKA_SASL_PASSWORD;
                break;
                
            case 'scram-sha-256':
            case 'scram-sha-512':
                config.sasl.username = process.env.KAFKA_SASL_USERNAME;
                config.sasl.password = process.env.KAFKA_SASL_PASSWORD;
                break;
                
            case 'aws':
                config.sasl.authorizationIdentity = process.env.KAFKA_SASL_AWS_ROLE;
                config.sasl.accessKeyId = process.env.KAFKA_SASL_AWS_ACCESS_KEY_ID;
                config.sasl.secretAccessKey = process.env.KAFKA_SASL_AWS_SECRET_ACCESS_KEY;
                config.sasl.sessionToken = process.env.KAFKA_SASL_AWS_SESSION_TOKEN;
                break;
                
            case 'oauthbearer':
                config.sasl.oauthBearerProvider = async () => {
                    const token = process.env.KAFKA_SASL_OAUTH_TOKEN;
                    if (!token) {
                        throw new Error('OAuth token not provided');
                    }
                    return {
                        value: token
                    };
                };
                break;
                
            default:
                console.warn(`Unsupported SASL mechanism: ${mechanism}`);
        }
        
        console.log(`SASL authentication enabled with mechanism: ${mechanism}`);
    }
    
    return config;
}

const kafka = new Kafka(createKafkaConfig());

// Schema Registry configuration
let schemaRegistry = null;
if (process.env.SCHEMA_REGISTRY_URL) {
    const schemaRegistryConfig = {
        host: process.env.SCHEMA_REGISTRY_URL,
    };
    
    // Add authentication if provided
    if (process.env.SCHEMA_REGISTRY_USERNAME && process.env.SCHEMA_REGISTRY_PASSWORD) {
        schemaRegistryConfig.auth = {
            username: process.env.SCHEMA_REGISTRY_USERNAME,
            password: process.env.SCHEMA_REGISTRY_PASSWORD,
        };
    }
    
    schemaRegistry = new SchemaRegistry(schemaRegistryConfig);
    console.log('Schema Registry configured:', process.env.SCHEMA_REGISTRY_URL);
} else {
    console.log('No Schema Registry URL provided - will attempt raw JSON parsing');
}

let consumer = null;
let isKafkaConnected = false;

// Serve the main HTML file
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Socket.io connection handling
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);
    
    // Send current connection status
    socket.emit('kafka-connected', isKafkaConnected);
    
    socket.on('connect-kafka', async (config = {}) => {
        try {
            await connectToKafka(socket, config);
        } catch (error) {
            console.error('Error connecting to Kafka:', error);
            socket.emit('kafka-error', error.message);
        }
    });
    
    socket.on('disconnect-kafka', async () => {
        try {
            await disconnectFromKafka();
            socket.emit('kafka-connected', false);
        } catch (error) {
            console.error('Error disconnecting from Kafka:', error);
        }
    });

    // Test data handler for development/testing
    socket.on('test-index-data', (data) => {
        console.log('Received test index data:', data);

        const indexData = {
            name: data.name,
            price_high: data.price_high,
            price_low: data.price_low,
            period_ending: data.period_ending,
            debug_info: data._debug,
            rawData: data
        };

        // Emit to all connected clients
        io.emit('index-data', indexData);
    });

    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });
});

async function lookupOffsetsByTimestamp(topicName, timestampRange) {
    const admin = kafka.admin();
    try {
        await admin.connect();
        console.log('Admin client connected for offset lookup');

        // Get topic metadata to find partitions
        const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
        const topic = metadata.topics.find(t => t.name === topicName);

        if (!topic) {
            throw new Error(`Topic ${topicName} not found`);
        }

        const partitions = topic.partitions.map(p => p.partitionId);
        console.log(`Found partitions for topic ${topicName}:`, partitions);

        await admin.disconnect();

        // Create a temporary consumer to look up offsets by timestamp
        const tempConsumer = kafka.consumer({ groupId: `offset-lookup-${Date.now()}` });
        await tempConsumer.connect();

        // Subscribe to the topic
        await tempConsumer.subscribe({ topic: topicName, fromBeginning: false });

        const startTimestamp = new Date(timestampRange.start).getTime();
        console.log(`Looking up offsets for timestamp: ${startTimestamp} (${timestampRange.start})`);

        // Build the topic partitions array for offset lookup
        const topicPartitions = partitions.map(partition => ({
            partition,
            timestamp: startTimestamp
        }));

        // Query offsets for the given timestamp
        const offsetResults = [];
        for (const tp of topicPartitions) {
            try {
                // Fetch the earliest and latest offsets for this partition
                const { low, high } = await tempConsumer.fetchOffsets({
                    topics: [{
                        topic: topicName,
                        partitions: [{ partition: tp.partition }]
                    }]
                });

                // For simplicity, start from the beginning if historical data is requested
                // In production, you'd want to implement binary search to find exact offset
                offsetResults.push({
                    partition: tp.partition,
                    offset: low || '0'
                });
                console.log(`Partition ${tp.partition}: Using offset ${low || '0'}`);
            } catch (error) {
                console.error(`Error fetching offset for partition ${tp.partition}:`, error.message);
                offsetResults.push({
                    partition: tp.partition,
                    offset: '0'
                });
            }
        }

        await tempConsumer.disconnect();
        console.log('Offset lookup results:', offsetResults);
        return offsetResults;

    } catch (error) {
        console.error('Error looking up offsets by timestamp:', error);
        await admin.disconnect().catch(console.error);
        throw error;
    }
}

async function connectToKafka(socket, config = {}) {
    if (consumer && isKafkaConnected) {
        console.log('Kafka consumer already connected');
        return;
    }
    
    try {
        console.log('Connecting to Kafka...');
        
        // Create consumer group ID from environment variable or default
        const baseGroupId = process.env.KAFKA_GROUP_ID || 'redpanda-visualizer-group';

        // Use unique group ID for timestamp-based consumption to avoid offset conflicts
        const groupId = config.timestampRange ?
            `${baseGroupId}-${Date.now()}` :
            baseGroupId;
        
        consumer = kafka.consumer({ 
            groupId: groupId,
            sessionTimeout: 30000,
            heartbeatInterval: 3000
        });
        
        await consumer.connect();
        console.log('Connected to Kafka');
        console.log('Consumer Group ID:', groupId);

        const topicName = process.env.KAFKA_TOPIC || 'market-data';

        if (config.timestampRange) {
            console.log('Historical mode: Timestamp-based consumption requested');
            console.log(`Time range: ${config.timestampRange.start} to ${config.timestampRange.end}`);

            // Subscribe from beginning for historical data
            await consumer.subscribe({
                topic: topicName,
                fromBeginning: true
            });

            const startTimestamp = new Date(config.timestampRange.start).getTime();
            const endTimestamp = new Date(config.timestampRange.end).getTime();
            console.log(`Historical mode: Filtering messages from ${startTimestamp} to ${endTimestamp}`);

            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    try {
                        // Get message timestamp
                        const messageTimestamp = message.timestamp ? parseInt(message.timestamp) : null;

                        // Skip messages outside the timestamp range
                        if (messageTimestamp) {
                            if (messageTimestamp < startTimestamp) {
                                // Message is before the start time, skip it
                                return;
                            }
                            if (messageTimestamp > endTimestamp) {
                                console.log('Historical mode: Reached end timestamp, stopping consumption');
                                await consumer.pause([{ topic: topicName }]);
                                return;
                            }
                        }

                        let data;

                        if (schemaRegistry) {
                            try {
                                // Try to decode using schema registry
                                data = await schemaRegistry.decode(message.value);
                                console.log('Decoded schema message:', data);
                            } catch (schemaError) {
                                console.log('Schema decode failed, trying raw JSON:', schemaError.message);
                                // Fallback to raw JSON parsing
                                const messageValue = message.value.toString();
                                data = JSON.parse(messageValue);
                            }
                        } else {
                            // Raw JSON parsing
                            const messageValue = message.value.toString();
                            console.log(`Received raw message: ${messageValue.substring(0, 200)}...`);
                            data = JSON.parse(messageValue);
                        }

                        // Process Redpanda Index data
                        if (data.name === 'Redpanda Index' && data.price_high !== undefined && data.price_low !== undefined) {
                            const indexData = {
                                name: data.name,
                                price_high: data.price_high,
                                price_low: data.price_low,
                                period_ending: data.period_ending || new Date().toISOString(),
                                debug_info: data._debug,
                                rawData: data
                            };

                            console.log('Emitting Redpanda Index data:', indexData);

                            // Emit to all connected clients
                            io.emit('index-data', indexData);
                        } else {
                            // Log first few messages to help debug
                            if (Math.random() < 0.01) { // Log ~1% of non-index messages
                                console.log('Sample non-index message:', {
                                    name: data.name,
                                    hasName: !!data.name,
                                    keys: Object.keys(data).slice(0, 10)
                                });
                            }
                        }
                    } catch (error) {
                        console.error('Error processing message:', error.message);
                        console.log('Raw message (first 200 chars):', message.value.toString().substring(0, 200));
                    }
                },
            });
        } else {
            // Real-time consumption
            await consumer.subscribe({
                topic: topicName,
                fromBeginning: false
            });

            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    try {
                        let data;

                        if (schemaRegistry) {
                            try {
                                // Try to decode using schema registry
                                data = await schemaRegistry.decode(message.value);
                                console.log('Decoded schema message:', data);
                            } catch (schemaError) {
                                console.log('Schema decode failed, trying raw JSON:', schemaError.message);
                                // Fallback to raw JSON parsing
                                const messageValue = message.value.toString();
                                data = JSON.parse(messageValue);
                            }
                        } else {
                            // Raw JSON parsing
                            const messageValue = message.value.toString();
                            console.log(`Received raw message: ${messageValue.substring(0, 200)}...`);
                            data = JSON.parse(messageValue);
                        }

                        // Process Redpanda Index data
                        if (data.name === 'Redpanda Index' && data.price_high !== undefined && data.price_low !== undefined) {
                            const indexData = {
                                name: data.name,
                                price_high: data.price_high,
                                price_low: data.price_low,
                                period_ending: data.period_ending || new Date().toISOString(),
                                debug_info: data._debug,
                                rawData: data
                            };

                            console.log('Emitting Redpanda Index data:', indexData);

                            // Emit to all connected clients
                            io.emit('index-data', indexData);
                        } else {
                            // Log first few messages to help debug
                            if (Math.random() < 0.01) { // Log ~1% of non-index messages
                                console.log('Sample non-index message:', {
                                    name: data.name,
                                    hasName: !!data.name,
                                    keys: Object.keys(data).slice(0, 10)
                                });
                            }
                        }
                    } catch (error) {
                        console.error('Error processing message:', error.message);
                        console.log('Raw message (first 200 chars):', message.value.toString().substring(0, 200));
                    }
                },
            });
        }
        
        console.log(`Subscribed to topic: ${topicName}`);
        
        isKafkaConnected = true;
        io.emit('kafka-connected', true);
        
        if (config.timestampRange) {
            console.log(`Kafka consumer running for timestamp range: ${config.timestampRange.start} to ${config.timestampRange.end}`);
        } else {
            console.log('Kafka consumer running in real-time mode...');
        }
        
    } catch (error) {
        console.error('Failed to connect to Kafka:', error);
        isKafkaConnected = false;
        io.emit('kafka-error', error.message);
        throw error;
    }
}

async function disconnectFromKafka() {
    if (consumer && isKafkaConnected) {
        try {
            console.log('Disconnecting from Kafka...');
            await consumer.disconnect();
            consumer = null;
            isKafkaConnected = false;
            console.log('Disconnected from Kafka');
        } catch (error) {
            console.error('Error disconnecting from Kafka:', error);
            throw error;
        }
    }
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('Shutting down gracefully...');
    await disconnectFromKafka();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('Shutting down gracefully...');
    await disconnectFromKafka();
    process.exit(0);
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
    console.log('Environment variables:');
    console.log('- KAFKA_TOPIC:', process.env.KAFKA_TOPIC || 'market-data (default)');
    console.log('- KAFKA_BROKERS:', process.env.KAFKA_BROKERS || 'localhost:9092 (default)');
    console.log('- KAFKA_CLIENT_ID:', process.env.KAFKA_CLIENT_ID || 'redpanda-visualizer (default)');
    console.log('- KAFKA_GROUP_ID:', process.env.KAFKA_GROUP_ID || 'redpanda-visualizer-group (default)');
    console.log('- KAFKA_SSL:', process.env.KAFKA_SSL || 'false (default)');
    console.log('- KAFKA_SASL_MECHANISM:', process.env.KAFKA_SASL_MECHANISM || 'none (default)');
    console.log('- SCHEMA_REGISTRY_URL:', process.env.SCHEMA_REGISTRY_URL || 'none (will use raw JSON parsing)');
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        kafkaConnected: isKafkaConnected,
        timestamp: new Date().toISOString()
    });
});