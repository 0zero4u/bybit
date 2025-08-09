// okx_listener_optimized.js
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
const SYMBOL = 'BTC-USDT';
const RECONNECT_INTERVAL_MS = 5000;
const SEND_INTERVAL_MS = 5; // New: Interval to send data to receiver
const MINIMUM_TICK_SIZE = 0.2; // This is now used to decide when to *update* the price, not when to send

// Using the correct internal DNS for service-to-service communication in GCP
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const EXCHANGE_STREAM_URL = 'wss://ws.okx.com:8443/ws/v5/public';

// --- WebSocket Clients and State ---
let internalWsClient, exchangeWsClient;
let last_sent_price = null;
let latest_best_bid_price = null; // New: Stores the most recent bid price from the exchange

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
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            // Error logging is eliminated
        }
    }
}

/**
 * Establishes and maintains the connection to the OKX WebSocket stream.
 */
function connectToExchange() {
    exchangeWsClient = new WebSocket(EXCHANGE_STREAM_URL);

    exchangeWsClient.on('open', () => {
        const subscriptionMessage = {
            op: "subscribe",
            args: [{
                channel: "bbo-tbt",
                instId: SYMBOL
            }]
        };
        try {
            exchangeWsClient.send(JSON.stringify(subscriptionMessage));
        } catch (e) {
            // Error logging is eliminated
        }
        last_sent_price = null;
        latest_best_bid_price = null;
    });

    exchangeWsClient.on('message', (data) => {
        if (data.toString() === 'pong') {
            return;
        }

        try {
            const message = JSON.parse(data.toString());

            if (message.arg && message.arg.channel === 'bbo-tbt' && message.data && message.data.length > 0) {
                const bboData = message.data[0];
                const bids = bboData.bids;

                if (bids && bids.length > 0) {
                    const bestBidPrice = parseFloat(bids[0]);

                    if (isNaN(bestBidPrice)) return;

                    // --- MODIFIED: Update the latest bid price if it meets the tick size criteria ---
                    const shouldUpdatePrice = (latest_best_bid_price === null) || (Math.abs(bestBidPrice - latest_best_bid_price) >= MINIMUM_TICK_SIZE);

                    if (shouldUpdatePrice) {
                        latest_best_bid_price = bestBidPrice;
                    }
                }
            }
        } catch (e) {
            // Error logging is eliminated
        }
    });

    exchangeWsClient.on('close', () => {
        exchangeWsClient = null; // Important to allow reconnection
        setTimeout(connectToExchange, RECONNECT_INTERVAL_MS);
    });

    const heartbeatInterval = setInterval(() => {
        if (exchangeWsClient && exchangeWsClient.readyState === WebSocket.OPEN) {
            try {
                exchangeWsClient.send('ping');
            } catch (e) {
                // Error logging is eliminated
            }
        } else {
            clearInterval(heartbeatInterval);
        }
    }, 25000);
}

// --- MODIFIED: New function to send data on a fixed interval ---
/**
 * Periodically sends the latest best bid price to the internal receiver.
 */
function startSendingInterval() {
    setInterval(() => {
        // Only send if there's a valid, new price to report
        if (latest_best_bid_price !== null && latest_best_bid_price !== last_sent_price) {
            payload_to_send.p = latest_best_bid_price;
            sendToInternalClient(payload_to_send);
            last_sent_price = latest_best_bid_price; // Update the last sent price
        }
    }, SEND_INTERVAL_MS);
}

// --- Script Entry Point ---
connectToInternalReceiver();
connectToExchange();
startSendingInterval(); // Start the new sending mechanism
