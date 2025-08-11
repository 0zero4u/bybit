
// data_receiver_server.js
const uWS = require('uWebSockets.js');
const axios = require('axios');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130;
// A topic name for the server's internal use. The client will never see or use this.
const PRICE_BROADCAST_TOPIC = 'all_clients_price_stream';

const BINANCE_TICKER_URL = 'https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT';

let listenSocketPublic, listenSocketInternal;
// The native pub/sub system replaces our manual client list. This is the "list" you wanted,
// but it's managed by the C++ core for maximum speed.
// const androidClients = new Set(); // <-- No longer needed.

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
        // This log is just for the server admin to see.
        // console.log(`[Receiver] Client from ${clientIp} auto-subscribed to price stream.`);
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
        // console.log(`[Receiver] Client disconnected and was auto-unsubscribed.`);
    }
})
.ws('/internal', {
    compression: uWS.DISABLED,
    maxPayloadLength: 4 * 1024,
    idleTimeout: 30,

    message: (ws, message, isBinary) => {
        // --- THE FASTEST PUSH ---
        // Publish the message ONCE to the topic. uWS then handles broadcasting
        // to all subscribed clients at the native C++ level. This is the "push together" action.
        app.publish(PRICE_BROADCAST_TOPIC, message, isBinary);
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

console.log(`[Receiver] PID: ${process.pid} --- Server initialized.`);
