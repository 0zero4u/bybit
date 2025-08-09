// bybit_listener_optimized.js
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
const MINIMUM_TICK_SIZE = 0.1;

// Using the correct internal DNS for service-to-service communication in GCP
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
// --- MODIFIED: Updated URL to Bybit V5 Spot stream ---
const EXCHANGE_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';

// --- WebSocket Clients and State ---
let internalWsClient, exchangeWsClient;
let last_sent_price = null;

// Optimization: Reusable payload object to prevent GC pressure.
const payload_to_send = { type: 'S', p: 0.0 };

/**
 * Establishes and maintains the connection to the internal WebSocket receiver.
 */
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;

    internalWsClient = new WebSocket(internalReceiverUrl);

    // --- LOG ELIMINATED ---
    // internalWsClient.on('error', (err) => console.error(`[Internal] WebSocket error: ${err.message}`));

    internalWsClient.on('close', () => {
        // --- LOG ELIMINATED ---
        // console.error('[Internal] Connection closed. Reconnecting...');
        internalWsClient = null; // Important to allow reconnection
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });

    // --- LOG ELIMINATED ---
    // internalWsClient.on('open', () => console.log('[Internal] Connection established.')); 
}

/**
 * Sends a payload to the internal WebSocket client.
 * @param {object} payload - The data to send.
 */
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            // The payload object is mutated and sent, not recreated.
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            // --- LOG ELIMINATED ---
            // console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    }
}

/**
 * Establishes and maintains the connection to the Bybit WebSocket stream.
 */
function connectToExchange() {
    exchangeWsClient = new WebSocket(EXCHANGE_STREAM_URL);

    exchangeWsClient.on('open', () => {
        // --- LOG ELIMINATED ---
        // console.log(`[Bybit] Connection established to: ${EXCHANGE_STREAM_URL}`);
        
        // --- MODIFIED: Subscribe to the orderbook topic for BTCUSDT ---
        const subscriptionMessage = {
            op: "subscribe",
            args: [`orderbook.1.${SYMBOL}`]
        };
        try {
            exchangeWsClient.send(JSON.stringify(subscriptionMessage));
            // --- LOG ELIMINATED ---
            // console.log(`[Bybit] Subscribed to ${subscriptionMessage.args[0]}`);
        } catch(e) {
            // --- LOG ELIMINATED ---
            // console.error(`[Bybit] Failed to send subscription message: ${e.message}`);
        }
        
        last_sent_price = null; // Reset on new connection
    });

    exchangeWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());

            // --- MODIFIED: Process Bybit orderbook data to get best bid price ---
            if (message.topic && message.topic.startsWith('orderbook.1') && message.data) {
                const bids = message.data.b;
                
                if (bids && bids.length > 0 && bids.length > 0) {
                    const bestBidPrice = parseFloat(bids);

                    if (isNaN(bestBidPrice)) return;

                    const shouldSendPrice = (last_sent_price === null) || (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE);

                    if (shouldSendPrice) {
                        // Optimization: Mutate the single payload object instead of creating a new one.
                        payload_to_send.p = bestBidPrice;
                        sendToInternalClient(payload_to_send);
                        last_sent_price = bestBidPrice;
                    }
                }
            }
        } catch (e) {
            // --- LOG ELIMINATED ---
            // console.error(`[Bybit] Error processing message: ${e.message}`);
        }
    });
    
    // --- LOG ELIMINATED ---
    // exchangeWsClient.on('error', (err) => console.error('[Bybit] Connection error:', err.message));

    exchangeWsClient.on('close', () => {
        // --- LOG ELIMINATED ---
        // console.error('[Bybit] Connection closed. Reconnecting...');
        exchangeWsClient = null; // Important to allow reconnection
        setTimeout(connectToExchange, RECONNECT_INTERVAL_MS);
    });
    
    // Bybit requires a ping every 20 seconds to keep the connection alive
    const heartbeatInterval = setInterval(() => {
        if (exchangeWsClient && exchangeWsClient.readyState === WebSocket.OPEN) {
            try {
                exchangeWsClient.send(JSON.stringify({ op: 'ping' }));
            } catch (e) {
                // --- LOG ELIMINATED ---
                // console.error(`[Bybit] Failed to send ping: ${e.message}`);
            }
        } else {
            clearInterval(heartbeatInterval);
        }
    }, 20000);
}

// --- Script Entry Point ---
// --- LOG ELIMINATED ---
// console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToExchange() ;
