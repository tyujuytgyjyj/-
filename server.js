const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

process.on('uncaughtException', (err) => {
    console.error('🔥 [حماية] السيرفر مستمر رغم الخطأ:', err.message);
});

// ─── الإعدادات ────────────────────────────────────────────────
const REQUEST_TIMEOUT_MS = 25000; // 25 ثانية قبل إعطاء 504
const HEARTBEAT_MS       = 15000; // فحص الاتصال كل 15 ثانية

// ─── الحالة الداخلية ──────────────────────────────────────────
const pendingRequests = new Map(); // { reqId → { res, timeoutHandle } }
let localClientSocket = null;

// ─── مساعدات ──────────────────────────────────────────────────

function sendErrorResponse(res, code, message) {
    try {
        if (!res.headersSent && !res.writableEnded) {
            res.writeHead(code, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end(`${code}: ${message}`);
        }
    } catch (_) { /* تجاهل إذا كان الاتصال مغلقاً */ }
}

function cleanupRequest(reqId) {
    const entry = pendingRequests.get(reqId);
    if (entry) {
        clearTimeout(entry.timeoutHandle);
        pendingRequests.delete(reqId);
    }
    return entry || null;
}

// أرسل خطأ لجميع الطلبات المعلقة ونظف الخريطة
function flushPendingRequests(code, message) {
    for (const [, entry] of pendingRequests) {
        clearTimeout(entry.timeoutHandle);
        sendErrorResponse(entry.res, code, message);
    }
    pendingRequests.clear();
}

// ─── HTTP Server ──────────────────────────────────────────────

const server = http.createServer((req, res) => {
    // --- فحص سريع: هل الكلاينت متصل؟ ---
    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        return sendErrorResponse(res, 502, 'الجهاز المحلي غير متصل.');
    }

    const reqId = crypto.randomUUID();
    const bodyChunks = [];

    req.on('data', (chunk) => bodyChunks.push(chunk));

    req.on('end', () => {
        // --- فحص ثانٍ بعد قراءة الجسم (قد يتغير الاتصال خلال القراءة) ---
        if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
            return sendErrorResponse(res, 502, 'الجهاز المحلي قطع الاتصال أثناء قراءة الطلب.');
        }

        const requestData = {
            id: reqId,
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: Buffer.concat(bodyChunks).toString('base64'),
            isBodyBase64: true,
        };

        // ⏱ مؤقت: إذا لم يرد الكلاينت خلال REQUEST_TIMEOUT_MS → 504
        const timeoutHandle = setTimeout(() => {
            const entry = cleanupRequest(reqId);
            if (entry) {
                console.warn(`⏱ Timeout (504): ${req.method} ${req.url}`);
                sendErrorResponse(entry.res, 504, 'Gateway Timeout: انتهت مهلة انتظار رد الكلاينت.');
            }
        }, REQUEST_TIMEOUT_MS);

        pendingRequests.set(reqId, { res, timeoutHandle });

        // ✉️ أرسل الطلب للكلاينت عبر WebSocket مع معالجة الأخطاء
        try {
            localClientSocket.send(JSON.stringify(requestData));
        } catch (err) {
            console.error('❌ فشل إرسال الطلب عبر WebSocket:', err.message);
            cleanupRequest(reqId);
            sendErrorResponse(res, 502, `فشل إرسال الطلب: ${err.message}`);
        }
    });

    req.on('error', (err) => {
        console.error('❌ خطأ في الطلب الوارد:', err.message);
        cleanupRequest(reqId);
        sendErrorResponse(res, 400, err.message);
    });

    // إذا أغلق المتصفح الصفحة قبل الرد، نظف المدخل لمنع تسرب الذاكرة
    req.on('close', () => {
        if (!res.writableEnded) cleanupRequest(reqId);
    });
});

// ─── WebSocket Server ─────────────────────────────────────────

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        // إذا اتصل كلاينت جديد، أغلق القديم بدل ما يتعارضوا
        if (localClientSocket && localClientSocket.readyState === WebSocket.OPEN) {
            console.warn('⚠️ كلاينت جديد. إغلاق القديم...');
            localClientSocket.close(1001, 'replaced by new client');
        }

        localClientSocket = ws;
        ws.isAlive = true; // لـ heartbeat
        console.log('🟩 تم ربط الكلاينت المحلي بنجاح!');

        ws.on('pong', () => { ws.isAlive = true; }); // رد على ping = الاتصال حي

        ws.on('message', (rawMessage) => {
            let responseData;
            try {
                responseData = JSON.parse(rawMessage.toString());
            } catch (e) {
                console.error('❌ رد غير قابل للتحليل:', e.message);
                return;
            }

            const entry = pendingRequests.get(responseData.id);
            if (!entry) return; // انتهت مهلته أو تم تنظيفه مسبقاً

            clearTimeout(entry.timeoutHandle);
            pendingRequests.delete(responseData.id);

            const { res } = entry;
            if (res.writableEnded) return; // المتصفح قفل قبل الرد

            // فك تشفير الجسم
            let body;
            if (responseData.isBase64 && responseData.body) {
                body = Buffer.from(responseData.body, 'base64');
            } else {
                body = Buffer.from(responseData.body || '');
            }

            // تنظيف الهيدرز وإصلاح content-length
            const headers = { ...(responseData.headers || {}) };
            delete headers['transfer-encoding']; // لأن الجسم مجمّع بالكامل
            headers['content-length'] = body.length;

            try {
                res.writeHead(responseData.status || 200, headers);
                res.end(body);
            } catch (e) {
                console.error('❌ خطأ في إرسال الرد للمتصفح:', e.message);
            }
        });

        ws.on('close', () => {
            console.log('🟥 الكلاينت فصل الاتصال. تفريغ الطلبات المعلقة...');
            if (localClientSocket === ws) localClientSocket = null;
            flushPendingRequests(502, 'انقطع الاتصال بالكلاينت المحلي.');
        });

        ws.on('error', (err) => {
            console.error('❌ خطأ في WebSocket الكلاينت:', err.message);
        });
    });
});

// ─── Heartbeat: كشف الاتصالات الميتة ─────────────────────────
// بعض الأحيان readyState === OPEN بس الاتصال TCP مات من داخل
// Ping/Pong كشف هذا خلال HEARTBEAT_MS

const heartbeatInterval = setInterval(() => {
    if (!localClientSocket) return;

    if (!localClientSocket.isAlive) {
        // لم يرد على آخر ping → اتصال ميت
        console.warn('💔 الكلاينت لم يرد على Ping. إنهاء الاتصال الميت...');
        localClientSocket.terminate();
        localClientSocket = null;
        flushPendingRequests(502, 'انقطع الاتصال (heartbeat فشل).');
        return;
    }

    // أرسل ping وانتظر pong
    localClientSocket.isAlive = false;
    try {
        localClientSocket.ping();
    } catch (err) {
        console.error('❌ فشل إرسال Ping:', err.message);
        localClientSocket.terminate();
        localClientSocket = null;
        flushPendingRequests(502, 'فشل heartbeat.');
    }
}, HEARTBEAT_MS);

server.on('close', () => clearInterval(heartbeatInterval));

// ─── Start ────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Proxy Server running on port ${PORT}`));
