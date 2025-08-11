
// binance_bookticker_listener.js
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
const SYMBOL = 'btcusdt'; // Binance uses lowercase for streams
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;

// Using the correct internal DNS for service-to-service communication in GCP
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
// --- MODIFIED: Updated URL to Binance Futures bookTicker stream ---
const EXCHANGE_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@bookTicker`;

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

    internalWsClient.on('close', () => {
        internalWsClient = null; // Important to allow reconnection
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
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
            // Error logging eliminated for performance
        }
    }
}

/**
 * Establishes and maintains the connection to the Binance WebSocket stream.
 */
function connectToExchange() {
    exchangeWsClient = new WebSocket(EXCHANGE_STREAM_URL);

    exchangeWsClient.on('open', () => {
        last_sent_price = null; // Reset on new connection
    });

    exchangeWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());

            // --- MODIFIED: Process Binance bookTicker stream data ---
            // bookTicker payload has a 'b' field for the best bid price. [1]
            if (message && message.b) {
                const bestBidPrice = parseFloat(message.b);

                if (isNaN(bestBidPrice)) return;

                const shouldSendPrice = (last_sent_price === null) || (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE);

                if (shouldSendPrice) {
                    // Optimization: Mutate the single payload object instead of creating a new one.
                    payload_to_send.p = bestBidPrice;
                    sendToInternalClient(payload_to_send);
                    last_sent_price = bestBidPrice;
                }
            }
        } catch (e) {
             // Error logging eliminated for performance
        }
    });

    exchangeWsClient.on('close', () => {
        exchangeWsClient = null; // Important to allow reconnection
        setTimeout(connectToExchange, RECONNECT_INTERVAL_MS);
    });

    // The 'ws' library automatically handles ping/pong frames from Binance.
    // No custom heartbeat is needed.
}

// --- Script Entry Point ---
connectToInternalReceiver();
connectToExchange();
