// binance_listener (60).js
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
    const clientsToTerminate = [internalWsClient, spotBookTickerClient, futuresBookTickerClient];
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
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;
const IMBALANCE_THRESHOLD_UPPER = 0.83;
const IMBALANCE_THRESHOLD_LOWER = 0.17;
const FAKE_PRICE_OFFSET = 1.0;
// MODIFIED: Added listening window duration
const IMBALANCE_WINDOW_MS = 50;

// --- URLS ---
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const SPOT_BOOKTICKER_URL = `wss://stream.binance.com:9443/ws/${SYMBOL}@bookTicker`;
const FUTURES_BOOKTICKER_URL = `wss://fstream.binance.com/ws/${SYMBOL}@bookTicker`;

// --- WebSocket Clients and State ---
let internalWsClient, spotBookTickerClient, futuresBookTickerClient;
let last_sent_price = null;
let latestSpotData = { b: null, B: null, a: null, A: null };
let last_fake_buy_price_sent = null;
let last_fake_sell_price_sent = null;
let lastFuturesTick = { b: null, a: null };

// --- MODIFIED: State for the listening window ---
// This object will hold the details of a pending check.
let pendingImbalanceCheck = null;

const payload_to_send = { type: 'S', p: 0.0 };

function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);
    internalWsClient.on('open', () => console.log('[Listener] Connected to internal receiver.'));
    internalWsClient.on('close', () => {
        console.log('[Listener] Disconnected from internal receiver. Reconnecting...');
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
    internalWsClient.on('error', (err) => console.error('[Listener] Internal receiver connection error:', err.message));
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) { /* Performance */ }
    }
}

function connectToSpotBookTicker() {
    spotBookTickerClient = new WebSocket(SPOT_BOOKTICKER_URL);
    spotBookTickerClient.on('open', () => {
        console.log('[Listener] Connected to Spot BookTicker Stream.');
        last_sent_price = null;
        last_fake_buy_price_sent = null;
        last_fake_sell_price_sent = null;
        // MODIFIED: Ensure pending check is clear on reconnect
        pendingImbalanceCheck = null;
    });

    spotBookTickerClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            if (!message || !message.b || !message.B || !message.a || !message.A) return;
            
            latestSpotData = message; // Always update latest spot data first

            // --- 1. Standard Price Tick Logic (Unchanged) ---
            const bestBidPrice = parseFloat(message.b);
            if (!isNaN(bestBidPrice)) {
                if ((last_sent_price === null) || (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE)) {
                    payload_to_send.p = bestBidPrice;
                    sendToInternalClient(payload_to_send);
                    last_sent_price = bestBidPrice;
                }
            }

            // --- 2. MODIFIED: Imbalance Check Logic (Moved here) ---
            // If a check is pending, see if this new spot data satisfies it.
            if (pendingImbalanceCheck) {
                // Check if the listening window has expired
                if (Date.now() > pendingImbalanceCheck.deadline) {
                    pendingImbalanceCheck = null; // Window closed, cancel the check
                    return;
                }

                const bidQty = parseFloat(message.B);
                const askQty = parseFloat(message.A);
                const totalQty = bidQty + askQty;
                if (totalQty === 0) return;
                const spotImbalance = bidQty / totalQty;

                if (pendingImbalanceCheck.direction === 'BUY' && spotImbalance >= IMBALANCE_THRESHOLD_UPPER) {
                    const fakePrice = pendingImbalanceCheck.basePrice + FAKE_PRICE_OFFSET;
                    if (fakePrice !== last_fake_buy_price_sent) {
                        payload_to_send.p = fakePrice;
                        payload_to_send.f = true;
                        sendToInternalClient(payload_to_send);
                        delete payload_to_send.f;
                        last_fake_buy_price_sent = fakePrice;
                        console.log(`[Listener] FAKE PRICE SENT (BUY): ${JSON.stringify({p: fakePrice, f: true})}`);
                    }
                    pendingImbalanceCheck = null; // Important: Fulfill and cancel check
                }
                else if (pendingImbalanceCheck.direction === 'SELL' && spotImbalance <= IMBALANCE_THRESHOLD_LOWER) {
                    const fakePrice = pendingImbalanceCheck.basePrice - FAKE_PRICE_OFFSET;
                     if (fakePrice !== last_fake_sell_price_sent) {
                        payload_to_send.p = fakePrice;
                        payload_to_send.f = true;
                        sendToInternalClient(payload_to_send);
                        delete payload_to_send.f;
                        last_fake_sell_price_sent = fakePrice;
                        console.log(`[Listener] FAKE PRICE SENT (SELL): ${JSON.stringify({p: fakePrice, f: true})}`);
                    }
                    pendingImbalanceCheck = null; // Important: Fulfill and cancel check
                }
            }
        } catch (e) { /* Performance */ }
    });
    spotBookTickerClient.on('close', () => {
        console.log('[Listener] Disconnected from Spot BookTicker. Reconnecting...');
        spotBookTickerClient = null;
        setTimeout(connectToSpotBookTicker, RECONNECT_INTERVAL_MS);
    });
    spotBookTickerClient.on('error', (err) => console.error('[Listener] Spot BookTicker error:', err.message));
}

function connectToFuturesBookTicker() {
    futuresBookTickerClient = new WebSocket(FUTURES_BOOKTICKER_URL);

    futuresBookTickerClient.on('open', () => {
        console.log('[Listener] Connected to Futures BookTicker Stream (Trigger).');
        lastFuturesTick = { b: null, a: null };
    });

    futuresBookTickerClient.on('message', (data) => {
        try {
            const f_message = JSON.parse(data.toString());
            if (!f_message || !f_message.b || !f_message.a) return;

            const newFuturesBid = parseFloat(f_message.b);
            const newFuturesAsk = parseFloat(f_message.a);

            if (lastFuturesTick.b === null) {
                lastFuturesTick = { b: newFuturesBid, a: newFuturesAsk };
                return;
            }

            // If a futures bid price has increased, it's an UP-tick.
            if (newFuturesBid > lastFuturesTick.b) {
                const spotBidPrice = parseFloat(latestSpotData.b);
                if(!isNaN(spotBidPrice)) {
                    // MODIFIED: Set a pending check instead of acting immediately
                    console.log(`[Listener] Futures UP-tick detected. Opening ${IMBALANCE_WINDOW_MS}ms listening window.`);
                    pendingImbalanceCheck = {
                        direction: 'BUY',
                        deadline: Date.now() + IMBALANCE_WINDOW_MS,
                        basePrice: spotBidPrice // Use spot price at the moment of the trigger
                    };
                }
            }
            // If a futures ask price has decreased, it's a DOWN-tick.
            else if (newFuturesAsk < lastFuturesTick.a) {
                 const spotAskPrice = parseFloat(latestSpotData.a);
                if(!isNaN(spotAskPrice)) {
                    // MODIFIED: Set a pending check instead of acting immediately
                    console.log(`[Listener] Futures DOWN-tick detected. Opening ${IMBALANCE_WINDOW_MS}ms listening window.`);
                    pendingImbalanceCheck = {
                        direction: 'SELL',
                        deadline: Date.now() + IMBALANCE_WINDOW_MS,
                        basePrice: spotAskPrice // Use spot price at the moment of the trigger
                    };
                }
            }

            lastFuturesTick = { b: newFuturesBid, a: newFuturesAsk };
        } catch (e) { /* Performance */ }
    });

    futuresBookTickerClient.on('close', () => {
        console.log('[Listener] Disconnected from Futures BookTicker. Reconnecting...');
        futuresBookTickerClient = null;
        setTimeout(connectToFuturesBookTicker, RECONNECT_INTERVAL_MS);
    });
    futuresBookTickerClient.on('error', (err) => console.error('[Listener] Futures BookTicker error:', err.message));
}


// --- Script Entry Point ---
connectToInternalReceiver();
connectToSpotBookTicker();
connectToFuturesBookTicker();
                  
