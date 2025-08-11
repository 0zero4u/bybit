// bitrue_listener.js
const WebSocket = require('ws');

// --- Process-wide Error Handling ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    cleanupAndExit(1);
});

/**
 * Gracefully terminates WebSocket clients and exits the process.
 * @param {number} [exitCode=1] - The exit code to use.
 */
function cleanupAndExit(exitCode = 1) {
    const clientsToTerminate = [internalWsClient, exchangeWsClient];
    console.error('[Listener] Initiating cleanup...');
    clientsToTerminate.forEach(client => {
        if (client && (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING)) {
            try {
                client.terminate();
            } catch (e) {
                console.error(`[Listener] Error during WebSocket termination: ${e.message}`);
            }
        }
    });
    // Allow time for cleanup before force-exiting
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL = 'BTCUSDT';
const RECONNECT_INTERVAL_MS = 5000;

// Using the correct internal DNS for service-to-service communication in GCP
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
// --- MODIFIED: Updated URL to Bitrue WebSocket stream ---
const EXCHANGE_STREAM_URL = 'wss://ws.bitrue.com/market/ws';

// --- WebSocket Clients and State ---
let internalWsClient, exchangeWsClient;
let heartbeatInterval = null;

/**
 * Establishes and maintains the connection to the internal WebSocket receiver.
 */
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;

    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('close', () => {
        internalWsClient = null; // Important to allow reconnection
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Sends raw data to the internal WebSocket client (no processing, just forwarding).
 * @param {Buffer} data - The raw data to send.
 */
function sendToInternalClient(data) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            // Send raw compressed data directly to receiver for decompression
            internalWsClient.send(data);
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    }
}

/**
 * Establishes and maintains the connection to the Bitrue WebSocket stream.
 */
function connectToExchange() {
    exchangeWsClient = new WebSocket(EXCHANGE_STREAM_URL);

    exchangeWsClient.on('open', () => {
        console.log(`[Bitrue] Connection established to: ${EXCHANGE_STREAM_URL}`);

        // --- MODIFIED: Subscribe to Bitrue orderbook channel ---
        const subscriptionMessage = {
            "event": "sub",
            "params": {
                "cb_id": "btcusdt",
                "channel": "market_btcusdt_simple_depth_step0"
            }
        };

        try {
            exchangeWsClient.send(JSON.stringify(subscriptionMessage));
            console.log(`[Bitrue] Subscribed to ${subscriptionMessage.params.channel}`);
        } catch(e) {
            console.error(`[Bitrue] Failed to send subscription message: ${e.message}`);
        }

        // Setup ping/pong heartbeat - Bitrue server sends ping every 15 seconds
        // We need to respond with pong within 1 minute
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
    });

    exchangeWsClient.on('message', (data, isBinary) => {
        // Check if this is a ping message from server
        if (!isBinary) {
            try {
                const textData = data.toString();
                if (textData === 'ping') {
                    // Respond to server ping with pong
                    exchangeWsClient.send('pong');
                    return;
                }
            } catch (e) {
                // Not a text message, continue with normal processing
            }
        }

        // Forward all market data (compressed) to internal receiver for decompression and processing
        sendToInternalClient(data);
    });

    exchangeWsClient.on('error', (err) => {
        console.error('[Bitrue] Connection error:', err.message);
    });

    exchangeWsClient.on('close', () => {
        console.error('[Bitrue] Connection closed. Reconnecting...');
        if (heartbeatInterval) {
            clearInterval(heartbeatInterval);
            heartbeatInterval = null;
        }
        exchangeWsClient = null; // Important to allow reconnection
        setTimeout(connectToExchange, RECONNECT_INTERVAL_MS);
    });
}

// --- Script Entry Point ---
console.log(`[Listener] Starting Bitrue Listener... PID: ${process.pid}`);
connectToInternalReceiver();
connectToExchange(); 
