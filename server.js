const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;
const pendingRequests = new Map();
let activeClient = null;

// إنشاء سيرفر الـ HTTP الأساسي
const server = http.createServer((req, res) => {
    try {
        // إذا كان جهازك المحلي غير متصل أو في حالة خمول
        if (!activeClient || activeClient.readyState !== 1) {
            res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
            return res.end('502 Bad Gateway: النفق مغلق، تأكد من تشغيل client.js في جهازك.');
        }

        const requestId = crypto.randomUUID();
        const chunks = [];

        req.on('data', chunk => chunks.push(chunk));
        req.on('end', () => {
            const bodyBuffer = Buffer.concat(chunks);
            
            const requestData = {
                id: requestId,
                method: req.method,
                url: req.url,
                headers: req.headers,
                body: bodyBuffer.toString('base64')
            };

            // مهلة أمان لمنع الـ 504 للمتصفح
            const timeout = setTimeout(() => {
                if (pendingRequests.has(requestId)) {
                    const pending = pendingRequests.get(requestId);
                    try {
                        pending.res.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
                        pending.res.end('504 Gateway Timeout: السيرفر المحلي تأخر في الاستجابة.');
                    } catch (e) {}
                    pendingRequests.delete(requestId);
                }
            }, 15000);

            pendingRequests.set(requestId, { res, timeout });
            
            // إرسال الطلب لجهازك مع معالجة الخطأ فوراً لو انقطع الاتصال بصمت
            try {
                activeClient.send(JSON.stringify(requestData), (err) => {
                    if (err) {
                        clearTimeout(timeout);
                        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                        res.end('502 Bad Gateway: فشل إرسال البيانات عبر النفق.');
                        pendingRequests.delete(requestId);
                    }
                });
            } catch (err) {
                clearTimeout(timeout);
                res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                res.end('502 Bad Gateway: النفق ميت.');
                pendingRequests.delete(requestId);
            }
        });
    } catch (globalHttpErr) {
        console.error('🚨 خطأ في استقبال طلب المتصفح:', globalHttpErr.message);
        if (!res.writableEnded) {
            res.writeHead(500);
            res.end('500 Internal Server Error');
        }
    }
});

// 🌟 الربط التلقائي والآمن للـ WebSocket لمنع خطأ 500 أثناء الـ Handshake
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
    console.log('🟩 تم ربط جهازك بنجاح والنفق مفتوح الآن!');
    activeClient = ws;
    ws.isAlive = true;

    // نظام نبضات القلب لمنع خمول السيرفر
    const pingInterval = setInterval(() => {
        if (ws.isAlive === false) {
            console.log('🟥 العميل لم يستجب، جاري قطع الاتصال الخامل.');
            return ws.terminate();
        }
        ws.isAlive = false;
        try { ws.ping(); } catch (e) { ws.terminate(); }
    }, 25000);

    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (message) => {
        try {
            const responseData = JSON.parse(message.toString());
            const pending = pendingRequests.get(responseData.id);

            if (pending) {
                clearTimeout(pending.timeout);
                
                const headers = responseData.headers || {};
                delete headers['connection'];
                delete headers['transfer-encoding'];

                pending.res.writeHead(responseData.status || 200, headers);
                pending.res.end(Buffer.from(responseData.body || '', 'base64'));
                
                pendingRequests.delete(responseData.id);
            }
        } catch (err) {
            console.error('❌ خطأ أثناء معالجة رد الكلاينت:', err.message);
        }
    });

    ws.on('close', () => {
        console.log('🟥 انقطع اتصال الجهاز المحلي بالنفق.');
        clearInterval(pingInterval);
        if (activeClient === ws) activeClient = null;
    });

    ws.on('error', (err) => {
        console.error('🚨 خطأ في السوكت:', err.message);
        ws.terminate();
    });
});

// منع انهيار السيرفر نهائياً عند حدوث أي خطأ عابر
process.on('uncaughtException', (err) => {
    console.error('🚨 خطأ غير متوقع في السيرفر الرئيسي تم تداركه:', err.message);
});

server.listen(PORT, () => {
    console.log(`🚀 سيرفر النفق يعمل بنجاح على بورت ${PORT}`);
});
