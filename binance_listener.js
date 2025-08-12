// binance_listener_optimized.js
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
    const clientsToTerminate = [internalWsClient, binanceWsClient];
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
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;

// Using the correct internal DNS for service-to-service communication in GCP
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
// --- MODIFIED: Updated URL to Binance Futures bookTicker stream ---
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@bookTicker`;

// --- WebSocket Clients and State ---
let internalWsClient, binanceWsClient;
let last_sent_trade_price = null;

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
            // The payload object is mutated and sent, not recreated.
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    }
}

/**
 * Establishes and maintains the connection to the Binance WebSocket stream.
 */
function connectToBinance() {
    // --- MODIFIED: Using the new Futures URL variable ---
    binanceWsClient = new WebSocket(BINANCE_FUTURES_STREAM_URL);
    
    binanceWsClient.on('open', () => {
        console.log(`[Binance] Connection established to stream: ${SYMBOL}@bookTicker`);
        last_sent_trade_price = null; // Reset on new connection
    });
    
    binanceWsClient.on('message', (data) => {
        try {
            const messageStr = data.toString();

            // Optimization: Manual string parsing to extract the best bid price.
            // We are looking for the pattern: "b":"<price>"
            const priceStartIndex = messageStr.indexOf('"b":"');
            if (priceStartIndex === -1) return; // Best bid price key not found

            const valueStartIndex = priceStartIndex + 5; // Move past '"b":"'
            const valueEndIndex = messageStr.indexOf('"', valueStartIndex);
            if (valueEndIndex === -1) return; // Closing quote not found

            const priceStr = messageStr.substring(valueStartIndex, valueEndIndex);
            const current_trade_price = parseFloat(priceStr);

            if (isNaN(current_trade_price)) return;

            const shouldSendPrice = (last_sent_trade_price === null) || (Math.abs(current_trade_price - last_sent_trade_price) >= MINIMUM_TICK_SIZE);

            if (shouldSendPrice) {
                // Optimization: Mutate the single payload object instead of creating a new one.
                payload_to_send.p = current_trade_price;
                sendToInternalClient(payload_to_send);
                last_sent_trade_price = current_trade_price;
            }
        } catch (e) { 
            console.error(`[Binance] Error processing message: ${e.message}`);
        }
    });

    binanceWsClient.on('error', (err) => console.error('[Binance] Connection error:', err.message));
    
    binanceWsClient.on('close', () => {
        console.error('[Binance] Connection closed. Reconnecting...');
        binanceWsClient = null; // Important to allow reconnection
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Script Entry Point ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToBinance();
