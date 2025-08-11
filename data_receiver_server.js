// data_receiver_server.js
const uWS = require('uWebSockets.js');
const axios = require('axios');
const zlib = require('zlib');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130;

// A topic name for the server's internal use. The client will never see or use this.
const PRICE_BROADCAST_TOPIC = 'all_clients_price_stream';

const BINANCE_TICKER_URL = 'https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT';

let listenSocketPublic, listenSocketInternal;

// Optimization: Reusable payload object to prevent GC pressure.
const payload_to_send = { type: 'S', p: 0.0 };
let last_sent_price = null;
const MINIMUM_TICK_SIZE = 0.1;

/**
 * Processes Bitrue orderbook data and extracts the best bid price
 * @param {object} bitrueData - The decompressed Bitrue orderbook data
 */
function processBitrueOrderbook(bitrueData) {
    try {
        // Bitrue orderbook format typically has 'tick' object with 'bids' array
        if (bitrueData && bitrueData.tick && bitrueData.tick.bids && bitrueData.tick.bids.length > 0) {
            // Bids are usually in format [price, volume] and sorted by price descending
            const bestBidPrice = parseFloat(bitrueData.tick.bids[0][0]);

            if (!isNaN(bestBidPrice)) {
                const shouldSendPrice = (last_sent_price === null) || 
                                      (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE);

                if (shouldSendPrice) {
                    // Optimization: Mutate the single payload object instead of creating a new one.
                    payload_to_send.p = bestBidPrice;
                    last_sent_price = bestBidPrice;
                    return JSON.stringify(payload_to_send);
                }
            }
        }
    } catch (e) {
        console.error(`[Receiver] Error processing Bitrue orderbook: ${e.message}`);
    }
    return null;
}

const app = uWS.App({});

app.ws('/public', {
    compression: uWS.SHARED_COMPRESSOR,
    maxPayloadLength: 16 * 1024,
    idleTimeout: IDLE_TIMEOUT_SECONDS,

    open: (ws) => {
        // --- THIS IS THE KEY ---
        // Implicitly subscribe every connecting client to the broadcast channel.
        // The client does not need to send any message. Its connection *is* the subscription.
        ws.subscribe(PRICE_BROADCAST_TOPIC);

        const clientIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
        console.log(`[Receiver] Client from ${clientIp} auto-subscribed to price stream.`);
    },

    message: async (ws, message, isBinary) => {
        // Handling for individual client requests remains the same.
        // This does not interfere with the broadcast.
        try {
            const request = JSON.parse(Buffer.from(message).toString());

            if (request.event === 'set_mode' && request.mode === 'semi_auto') {
                const response = await axios.get(BINANCE_TICKER_URL);
                const lastPrice = parseFloat(response.data.lastPrice);

                if (!isNaN(lastPrice)) {
                    const payload = { type: 'S', p: lastPrice };
                    try {
                       ws.send(JSON.stringify(payload), isBinary);
                    } catch (e) {
                       console.error(`[Receiver] Error sending instant price to client: ${e.message}`);
                    }
                }
            }
        } catch (e) {
            // Irrelevant message.
        }
    },

    close: (ws, code, message) => {
        // uWebSockets.js automatically handles unsubscribing the client. No action needed.
        console.log(`[Receiver] Client disconnected and was auto-unsubscribed.`);
    }
})
.ws('/internal', {
    compression: uWS.DISABLED,
    maxPayloadLength: 64 * 1024, // Increased for compressed data
    idleTimeout: 30,

    message: (ws, message, isBinary) => {
        // --- GZIP DECOMPRESSION ADDED ---
        // The listener sends compressed data from Bitrue, we need to decompress it here
        try {
            let decompressedData;

            if (isBinary) {
                // Decompress gzipped data synchronously
                const buffer = Buffer.from(message);
                decompressedData = zlib.gunzipSync(buffer);
            } else {
                // If not binary, treat as regular JSON
                decompressedData = Buffer.from(message);
            }

            // Parse the decompressed JSON
            const bitrueMessage = JSON.parse(decompressedData.toString());

            // Process the Bitrue orderbook data
            const processedPayload = processBitrueOrderbook(bitrueMessage);

            if (processedPayload) {
                // --- THE FASTEST PUSH ---
                // Publish the processed message ONCE to the topic. uWS then handles broadcasting
                // to all subscribed clients at the native C++ level. This is the "push together" action.
                app.publish(PRICE_BROADCAST_TOPIC, processedPayload, false);
            }
        } catch (e) {
            console.error(`[Receiver] Error decompressing/processing internal message: ${e.message}`);

            // Fallback: try to process as uncompressed JSON
            try {
                const fallbackMessage = JSON.parse(Buffer.from(message).toString());
                const processedPayload = processBitrueOrderbook(fallbackMessage);

                if (processedPayload) {
                    app.publish(PRICE_BROADCAST_TOPIC, processedPayload, false);
                }
            } catch (fallbackError) {
                console.error(`[Receiver] Fallback processing also failed: ${fallbackError.message}`);
            }
        }
    }
    // 'open' and 'close' handlers for the internal listener can be minimal
})
.listen(PUBLIC_PORT, (token) => {
    listenSocketPublic = token;
    if (token) console.log(`[Receiver] Public WebSocket server listening on port ${PUBLIC_PORT}`);
    else {
        console.error(`[Receiver] FAILED to listen on port ${PUBLIC_PORT}`);
        process.exit(1);
    }
})
.listen(INTERNAL_LISTENER_PORT, (token) => {
    listenSocketInternal = token;
    if (token) console.log(`[Receiver] Internal WebSocket server listening on port ${INTERNAL_LISTENER_PORT}`);
    else {
        console.error(`[Receiver] FAILED to listen on port ${INTERNAL_LISTENER_PORT}`);
        process.exit(1);
    }
});

function shutdown() {
    console.log('[Receiver] Shutting down...');
    if (listenSocketPublic) uWS.us_listen_socket_close(listenSocketPublic);
    if (listenSocketInternal) uWS.us_listen_socket_close(listenSocketInternal);
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(`[Receiver] PID: ${process.pid} --- Server initialized with Bitrue support and gzip decompression.`);
            
