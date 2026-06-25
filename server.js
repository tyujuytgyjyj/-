const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

// 🛡️ درع حماية شامل
process.on('uncaughtException', (err) => {
    console.error('🔥 [حماية] السيرفر مستمر رغم الخطأ:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('🔥 [unhandledRejection]:', err);
});

// ⚙️ الإعدادات
const PORT = process.env.PORT || 3000;
const AUTH_TOKEN = process.env.AUTH_TOKEN || '';
const REQUEST_TIMEOUT_MS = 30000;       // ⏱️ timeout لكل طلب
const HEARTBEAT_INTERVAL_MS = 10000;    // 💓 فحص كل 10 ثوانٍ
const MAX_QUEUE_WAIT_MS = 5000;         // 📥 انتظار الاتصال حتى 5 ثوانٍ (يكفي لإعادة اتصال Render)
const MAX_BACKPRESSURE = 5 * 1024 * 1024; // 5MB

// 🗂️ حالة الاتصال الحالي
let activeConnection = null;

// 📥 قائمة انتظار للطلبات خلال الانقطاع المؤقت
const connectionWaitQueue = [];

const server = http.createServer((req, res) => {
    const reqId = crypto.randomUUID();
    const bodyChunks = [];

    req.on('data', chunk => bodyChunks.push(chunk));

    req.on('end', () => {
        const requestData = {
            id: reqId,
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: Buffer.concat(bodyChunks).toString('base64'),
            isBodyBase64: true
        };
        attemptSend(reqId, requestData, res);
    });

    // 🌟 لو المتصفح ألغى الطلب قبل اكتمال الرد
    // ملاحظة: نستخدم res.on('close') لأن req.on('close') يُطلق بعد end مباشرة في Node.js الحديث
    res.on('close', () => {
        if (!res.writableEnded) {
            removeFromAllPending(reqId);
        }
    });

    req.on('error', () => {
        if (!res.writableEnded) {
            try { res.writeHead(400); res.end(); } catch (e) {}
        }
    });
});

// 🚀 محاولة إرسال الطلب للكلاينت
function attemptSend(reqId, requestData, res) {
    const conn = activeConnection;

    // 🛡️ لا اتصال حي → انتظار
    if (!conn || conn.ws.readyState !== WebSocket.OPEN) {
        queueRequest(reqId, requestData, res);
        return;
    }

    // 🛡️ backpressure عالية → انتظار
    if (conn.ws.bufferedAmount > MAX_BACKPRESSURE) {
        console.warn(`⚠️ [backpressure ${conn.ws.bufferedAmount}B] تأجيل ${reqId.slice(0,8)}`);
        queueRequest(reqId, requestData, res);
        return;
    }

    // ✅ إرسال مباشر
    sendToConnection(conn, reqId, requestData, res);
}

// 📤 إرسال لاتصال محدد + تخزين requestData للنقل المستقبلي
function sendToConnection(conn, reqId, requestData, res) {
    const timer = setTimeout(() => {
        const pending = conn.pending.get(reqId);
        if (pending && !pending.res.writableEnded) {
            console.error(`⏱️ [timeout] ${reqId.slice(0,8)} لم يصل رده`);
            pending.res.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
            pending.res.end('504 Gateway Timeout: الكلاينت المحلي لم يرد.');
        }
        conn.pending.delete(reqId);
    }, REQUEST_TIMEOUT_MS);

    // 🌟 نخزّن requestData كاملاً في pending ليمكن نقله لاتصال جديد لو انقطع الحالي
    conn.pending.set(reqId, { res, timer, requestData, conn });

    conn.ws.send(JSON.stringify(requestData), (sendErr) => {
        if (sendErr) {
            console.error(`📤 [send error] ${reqId.slice(0,8)}:`, sendErr.message);
            const pending = conn.pending.get(reqId);
            if (pending && !pending.res.writableEnded) {
                clearTimeout(pending.timer);
                conn.pending.delete(reqId);
                // 🔄 أعد للمحاولة عبر قائمة الانتظار
                queueRequest(reqId, requestData, res, true);
            }
        }
    });
}

// 📥 وضع طلب في قائمة الانتظار
function queueRequest(reqId, requestData, res, isRetry = false) {
    if (res.writableEnded) return;

    const entry = { reqId, requestData, res, queuedAt: Date.now(), isRetry, failTimer: null };
    connectionWaitQueue.push(entry);

    entry.failTimer = setTimeout(() => {
        const idx = connectionWaitQueue.indexOf(entry);
        if (idx === -1) return;
        connectionWaitQueue.splice(idx, 1);
        if (!res.writableEnded) {
            res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('502 Bad Gateway: الجهاز المحلي غير متصل (انتهى وقت الانتظار).');
        }
    }, MAX_QUEUE_WAIT_MS);
}

// 🔄 تفريغ قائمة الانتظار
function flushWaitQueue() {
    if (connectionWaitQueue.length === 0) return;
    const conn = activeConnection;
    if (!conn || conn.ws.readyState !== WebSocket.OPEN) return;

    const count = connectionWaitQueue.length;
    console.log(`🔄 [flush] إرسال ${count} طلب مؤجّل...`);

    const toFlush = connectionWaitQueue.splice(0);
    for (const entry of toFlush) {
        if (entry.res.writableEnded) {
            clearTimeout(entry.failTimer);
            continue;
        }
        if (conn.ws.bufferedAmount > MAX_BACKPRESSURE) {
            // أعد للقائمة وحاول بعد 100ms
            connectionWaitQueue.push(entry);
            setTimeout(flushWaitQueue, 100);
            continue;
        }
        clearTimeout(entry.failTimer);
        sendToConnection(conn, entry.reqId, entry.requestData, entry.res);
    }
}

// 🧹 إزالة طلب من كل المواقع
function removeFromAllPending(reqId) {
    if (activeConnection && activeConnection.pending) {
        const pending = activeConnection.pending.get(reqId);
        if (pending) {
            clearTimeout(pending.timer);
            activeConnection.pending.delete(reqId);
            return;
        }
    }
    const idx = connectionWaitQueue.findIndex(e => e.reqId === reqId);
    if (idx !== -1) {
        const entry = connectionWaitQueue[idx];
        clearTimeout(entry.failTimer);
        connectionWaitQueue.splice(idx, 1);
    }
}

// 🔄 ترقية WebSocket
const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    if (AUTH_TOKEN) {
        const url = new URL(request.url, 'http://localhost');
        const token = url.searchParams.get('token') || request.headers['x-proxy-token'];
        if (token !== AUTH_TOKEN) {
            socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            socket.destroy();
            return;
        }
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
        const connId = crypto.randomUUID();
        const conn = {
            id: connId,
            ws: ws,
            pending: new Map(),
            isAlive: true,
            heartbeatInterval: null
        };

        // 🚨 استبدال الاتصال القديم — مع نقل الطلبات المعلقة للقائمة (لا فشلها!)
        if (activeConnection) {
            const oldConn = activeConnection;
            console.log('🔄 اتصال جديد — استبدال القديم...');
            if (oldConn.heartbeatInterval) clearInterval(oldConn.heartbeatInterval);

            // 🌟 المفتاح: نقل الطلبات المعلقة للقائمة بدل فشلها
            if (oldConn.pending && oldConn.pending.size > 0) {
                console.log(`📦 نقل ${oldConn.pending.size} طلب معلّق للقائمة...`);
                for (const [reqId, pending] of oldConn.pending.entries()) {
                    clearTimeout(pending.timer);
                    if (!pending.res.writableEnded && pending.requestData) {
                        // أعد للقائمة بدون timer قصير — ستحاول flushWaitQueue إرسالها فوراً
                        const entry = {
                            reqId,
                            requestData: pending.requestData,
                            res: pending.res,
                            queuedAt: Date.now(),
                            isRetry: true,
                            failTimer: setTimeout(() => {
                                const idx = connectionWaitQueue.indexOf(entry);
                                if (idx === -1) return;
                                connectionWaitQueue.splice(idx, 1);
                                if (!pending.res.writableEnded) {
                                    pending.res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                                    pending.res.end('502 Bad Gateway: فشل إعادة الإرسال.');
                                }
                            }, MAX_QUEUE_WAIT_MS)
                        };
                        connectionWaitQueue.push(entry);
                    }
                }
                oldConn.pending.clear();
            }

            try { oldConn.ws.removeAllListeners('close'); oldConn.ws.close(); } catch (e) {}
        }

        activeConnection = conn;
        console.log(`🟩 [${connId.slice(0,8)}] تم ربط الكلاينت المحلي!`);

        // 💓 Heartbeat
        conn.heartbeatInterval = setInterval(() => {
            if (conn.ws.readyState !== WebSocket.OPEN) return;
            if (!conn.isAlive) {
                console.log(`💀 [${connId.slice(0,8)}] لم يصل pong — إغلاق قسري.`);
                conn.ws.terminate();
                return;
            }
            conn.isAlive = false;
            try { conn.ws.ping(); } catch (e) {}
        }, HEARTBEAT_INTERVAL_MS);

        ws.on('pong', () => { conn.isAlive = true; });

        ws.on('message', (message) => {
            try {
                const responseData = JSON.parse(message.toString());
                const pending = conn.pending.get(responseData.id);
                if (!pending) return;
                if (!pending.res.writableEnded) {
                    let finalBody = responseData.body || '';
                    if (responseData.isBase64 && responseData.body) {
                        finalBody = Buffer.from(responseData.body, 'base64');
                    }
                    clearTimeout(pending.timer);
                    pending.res.writeHead(responseData.status, responseData.headers);
                    pending.res.end(finalBody);
                }
                conn.pending.delete(responseData.id);
            } catch (e) {
                console.error('خطأ في معالجة الرد:', e.message);
            }
        });

        ws.on('close', () => {
            console.log(`🟥 [${connId.slice(0,8)}] فصل. طلبات معلقة: ${conn.pending.size}.`);
            if (activeConnection === conn) {
                activeConnection = null;
            }
            if (conn.heartbeatInterval) clearInterval(conn.heartbeatInterval);

            // 🌟 نقل الطلبات للقائمة بدل فشلها (لو عاد الاتصال خلال 3 ثوانٍ، ستعمل)
            if (conn.pending.size > 0) {
                console.log(`📦 نقل ${conn.pending.size} طلب للقائمة بانتظار إعادة الاتصال...`);
                for (const [reqId, pending] of conn.pending.entries()) {
                    clearTimeout(pending.timer);
                    if (!pending.res.writableEnded && pending.requestData) {
                        const entry = {
                            reqId,
                            requestData: pending.requestData,
                            res: pending.res,
                            queuedAt: Date.now(),
                            isRetry: true,
                            failTimer: null
                        };
                        entry.failTimer = setTimeout(() => {
                            const idx = connectionWaitQueue.indexOf(entry);
                            if (idx === -1) return;
                            connectionWaitQueue.splice(idx, 1);
                            if (!pending.res.writableEnded) {
                                pending.res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                                pending.res.end('502 Bad Gateway: انقطع الاتصال ولم يعد.');
                            }
                        }, MAX_QUEUE_WAIT_MS);
                        connectionWaitQueue.push(entry);
                    }
                }
                conn.pending.clear();
            }
        });

        ws.on('error', (err) => {
            console.error(`⚠️ [${connId.slice(0,8)}] ws error:`, err.message);
        });

        // 🔄 تفريغ قائمة الانتظار فور توفر الاتصال الجديد
        flushWaitQueue();
    });
});

// 🛠️ فشل كل الطلبات (للطوارئ فقط)
function failAllPending(conn, status, message) {
    if (!conn || !conn.pending) return;
    for (const [id, pending] of conn.pending.entries()) {
        if (pending.timer) clearTimeout(pending.timer);
        if (!pending.res.headersSent && !pending.res.writableEnded) {
            try {
                pending.res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8' });
                pending.res.end(message);
            } catch (e) {}
        }
    }
    conn.pending.clear();
}

// 📊 تقرير دوري
setInterval(() => {
    const pendingCount = activeConnection ? activeConnection.pending.size : 0;
    const queueCount = connectionWaitQueue.length;
    const connState = activeConnection
        ? (activeConnection.ws.readyState === WebSocket.OPEN ? 'OPEN' : 'CLOSING/CLOSED')
        : 'لا اتصال';
    const bp = activeConnection ? activeConnection.ws.bufferedAmount : 0;
    console.log(`📊 [status] اتصال: ${connState} | معلقة: ${pendingCount} | في الانتظار: ${queueCount} | bp: ${bp}B`);
}, 30000);

server.listen(PORT, () => {
    console.log(`🚀 Proxy running on port ${PORT}`);
    console.log(`   ⏱️  request timeout: ${REQUEST_TIMEOUT_MS}ms`);
    console.log(`   💓 heartbeat: كل ${HEARTBEAT_INTERVAL_MS}ms`);
    console.log(`   📥 queue wait: ${MAX_QUEUE_WAIT_MS}ms`);
    console.log(`   🌊 max backpressure: ${MAX_BACKPRESSURE / 1024 / 1024}MB`);
    if (AUTH_TOKEN) console.log(`   🔒 المصادقة بالتوكن مفعّلة`);
});
