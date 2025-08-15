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
    const clientsToTerminate = [internalWsClient, bybitWsClient, okxWsClient, binanceWsClient];
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
const SYMBOL_BYBIT = 'BTCUSDT';
const SYMBOL_OKX = 'BTC-USDT';
const SYMBOL_BINANCE = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const AVERAGE_PRICE_CHANGE_THRESHOLD = 1.0; // New threshold for sending average price

// Using the correct internal DNS for service-to-service communication in GCP
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';

// --- Exchange Stream URLs ---
const BYBIT_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';
const OKX_STREAM_URL = 'wss://ws.okx.com:8443/ws/v5/public';
const BINANCE_STREAM_URL = `wss://stream.binance.com:9443/ws/${SYMBOL_BINANCE}@trade`;


// --- WebSocket Clients and State ---
let internalWsClient, bybitWsClient, okxWsClient, binanceWsClient;
let last_sent_price = null;
let latest_prices = {
    bybit: null,
    okx: null,
    binance: null
};


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
            // Error logging eliminated
        }
    }
}


/**
 * Calculates and sends the average price if the change threshold is met.
 */
function calculateAndSendAverage() {
    const valid_prices = Object.values(latest_prices).filter(p => p !== null);
    if (valid_prices.length === 0) return;

    const average_price = valid_prices.reduce((sum, p) => sum + p, 0) / valid_prices.length;

    const shouldSendPrice = (last_sent_price === null) || (Math.abs(average_price - last_sent_price) >= AVERAGE_PRICE_CHANGE_THRESHOLD);

    if (shouldSendPrice) {
        payload_to_send.p = average_price;
        sendToInternalClient(payload_to_send);
        last_sent_price = average_price;
    }
}

/**
 * Establishes and maintains the connection to the Bybit WebSocket stream.
 */
function connectToBybit() {
    bybitWsClient = new WebSocket(BYBIT_STREAM_URL);

    bybitWsClient.on('open', () => {
        const subscriptionMessage = {
            op: "subscribe",
            args: [`publicTrade.${SYMBOL_BYBIT}`]
        };
        try {
            bybitWsClient.send(JSON.stringify(subscriptionMessage));
        } catch(e) {
            // Error logging eliminated
        }
    });

    bybitWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.topic && message.topic.startsWith('publicTrade') && message.data) {
                const tradePrice = parseFloat(message.data[0].p);
                if (!isNaN(tradePrice)) {
                    latest_prices.bybit = tradePrice;
                    calculateAndSendAverage();
                }
            }
        } catch (e) {
            // Error logging eliminated
        }
    });
    
    bybitWsClient.on('close', () => {
        latest_prices.bybit = null;
        bybitWsClient = null;
        setTimeout(connectToBybit, RECONNECT_INTERVAL_MS);
    });

    const heartbeatInterval = setInterval(() => {
        if (bybitWsClient && bybitWsClient.readyState === WebSocket.OPEN) {
            try {
                bybitWsClient.send(JSON.stringify({ op: 'ping' }));
            } catch (e) {
                // Error logging eliminated
            }
        } else {
            clearInterval(heartbeatInterval);
        }
    }, 20000);
}


/**
 * Establishes and maintains the connection to the OKX WebSocket stream.
 */
function connectToOkx() {
    okxWsClient = new WebSocket(OKX_STREAM_URL);

    okxWsClient.on('open', () => {
        const subscriptionMessage = {
            op: "subscribe",
            args: [{ channel: "trades", instId: SYMBOL_OKX }]
        };
        try {
            okxWsClient.send(JSON.stringify(subscriptionMessage));
        } catch (e) {
             // Error logging eliminated
        }
    });

    okxWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (message.arg && message.arg.channel === 'trades' && message.data) {
                const tradePrice = parseFloat(message.data[0].px);
                if (!isNaN(tradePrice)) {
                    latest_prices.okx = tradePrice;
                    calculateAndSendAverage();
                }
            }
        } catch (e) {
             // Error logging eliminated
        }
    });

    okxWsClient.on('close', () => {
        latest_prices.okx = null;
        okxWsClient = null;
        setTimeout(connectToOkx, RECONNECT_INTERVAL_MS);
    });
    
    const heartbeatInterval = setInterval(() => {
        if (okxWsClient && okxWsClient.readyState === WebSocket.OPEN) {
            try {
                okxWsClient.send('ping');
            } catch (e) {
                 // Error logging eliminated
            }
        } else {
            clearInterval(heartbeatInterval);
        }
    }, 25000);
}


/**
 * Establishes and maintains the connection to the Binance WebSocket stream.
 */
function connectToBinance() {
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);

    binanceWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            // Binance trade stream does not have a topic/channel field in the message itself
            if (message.p) {
                const tradePrice = parseFloat(message.p);
                if (!isNaN(tradePrice)) {
                    latest_prices.binance = tradePrice;
                    calculateAndSendAverage();
                }
            }
        } catch (e) {
            // Error logging eliminated
        }
    });

    binanceWsClient.on('close', () => {
        latest_prices.binance = null;
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}


// --- Script Entry Point ---
connectToInternalReceiver();
connectToBybit();
connectToOkx();
connectToBinance();
