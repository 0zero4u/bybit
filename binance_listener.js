// bybit_listener.js
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
    const clientsToTerminate = [internalWsClient, bybitWsClient];
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
const SYMBOL = 'BTCUSDT'; // Bybit uses uppercase symbols
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;
const SEND_INTERVAL_MS = 20;

// Using the correct internal DNS for service-to-service communication in GCP
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BYBIT_FUTURES_STREAM_URL = 'wss://stream.bybit.com/v5/public/linear';

// --- WebSocket Clients and State ---
let internalWsClient, bybitWsClient;
let last_sent_trade_price = null;
let price_buffer = null;
let heartbeatInterval = null;

// Optimization: Reusable payload object to prevent GC pressure.
const payload_to_send = { type: 'S', p: 0.0 };

/**
 * Establishes and maintains the connection to the internal WebSocket receiver.
 */
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('error', (err) => console.error(`[Internal] WebSocket error: ${err.message}`));
    
    internalWsClient.on('close', () => {
        console.error('[Internal] Connection closed. Reconnecting...');
        internalWsClient = null; // Important to allow reconnection
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
    
    internalWsClient.on('open', () => console.log('[Internal] Connection established.'));
}

/**
 * Sends a payload to the internal WebSocket client.
 * @param {object} payload - The data to send.
 */
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    }
}

/**
 * Establishes and maintains the connection to the Bybit WebSocket stream.
 */
function connectToBybit() {
    bybitWsClient = new WebSocket(BYBIT_FUTURES_STREAM_URL);
    
    bybitWsClient.on('open', () => {
        console.log(`[Bybit] Connection established to stream: ${BYBIT_FUTURES_STREAM_URL}`);
        
        // Subscribe to the order book topic
        const subscriptionMessage = {
            op: "subscribe",
            args: [`orderbook.1.${SYMBOL}`]
        };
        bybitWsClient.send(JSON.stringify(subscriptionMessage));
        console.log(`[Bybit] Subscribed to orderbook.1.${SYMBOL}`);

        last_sent_trade_price = null; // Reset on new connection
        price_buffer = null;

        // Bybit requires a ping every 20 seconds to keep the connection alive
        if (heartbeatInterval) clearInterval(heartbeatInterval);
        heartbeatInterval = setInterval(() => {
            if (bybitWsClient && bybitWsClient.readyState === WebSocket.OPEN) {
                try {
                    bybitWsClient.send(JSON.stringify({ op: 'ping' }));
                } catch (e) {
                    console.error(`[Bybit] Failed to send ping: ${e.message}`);
                }
            }
        }, 20000);
    });
    
    bybitWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            
            // Handle subscription confirmation and order book data
            if (message.topic && message.topic.startsWith('orderbook.1') && message.data) {
                const { b: bids } = message.data;
                if (bids && bids.length > 0) {
                    const current_trade_price = parseFloat(bids[0][0]);

                    if (!isNaN(current_trade_price)) {
                        // Keep updating the buffer with the very latest price received.
                        price_buffer = current_trade_price;
                    }
                }
            }
        } catch (e) { 
            console.error(`[Bybit] Error processing message: ${e.message}`);
        }
    });

    bybitWsClient.on('error', (err) => console.error('[Bybit] Connection error:', err.message));
    
    bybitWsClient.on('close', () => {
        console.error('[Bybit] Connection closed. Reconnecting...');
        if (heartbeatInterval) clearInterval(heartbeatInterval);
        bybitWsClient = null;
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Schedules sending the buffered price at a fixed interval.
 * This function checks the latest price in the buffer every SEND_INTERVAL_MS.
 * It sends the price only if it has changed by the minimum tick size
 * from the last price that was actually sent.
 */
function startSendingScheduler() {
    setInterval(() => {
        if (price_buffer === null) {
            return; // No new price has been received yet.
        }

        const shouldSendPrice = (last_sent_trade_price === null) || 
                                (Math.abs(price_buffer - last_sent_trade_price) >= MINIMUM_TICK_SIZE);

        if (shouldSendPrice) {
            payload_to_send.p = price_buffer;
            sendToInternalClient(payload_to_send);
            last_sent_trade_price = price_buffer;
        }
    }, SEND_INTERVAL_MS);
}


// --- Script Entry Point ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToBybit();
startSendingScheduler();
