const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;
const pendingRequests = new Map();
let activeClient = null;

// 1. استقبال طلبات الـ HTTP من المتصفح
const server = http.createServer((req, res) => {
    // إذا كان العميل غير متصل أو الاتصال ميت، نرد فوراً بـ 502 بدلاً من التعليق
    if (!activeClient || activeClient.readyState !== 1) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: جهازك المحلي غير متصل بالنفق حالياً أو في حالة خمول.');
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

        // 🌟 حماية قصوى: مهلة 15 ثانية فقط لقطع الشك باليقين والرد قبل أن يتدخل سيرفر Render بـ 504
        const timeout = setTimeout(() => {
            if (pendingRequests.has(requestId)) {
                const pending = pendingRequests.get(requestId);
                try {
                    pending.res.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
                    pending.res.end('504 Gateway Timeout: السيرفر المحلي استغرق وقتاً طويلاً جداً ولم يستجب.');
                } catch (e) {}
                pendingRequests.delete(requestId);
            }
        }, 15000);

        pendingRequests.set(requestId, { res, timeout });
        
        // إرسال الطلب مع فحص فوري لوجود خطأ في الإرسال
        try {
            activeClient.send(JSON.stringify(requestData), (err) => {
                if (err) {
                    clearTimeout(timeout);
                    res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                    res.end('502 Bad Gateway: فشل إرسال البيانات عبر النفق الميت.');
                    pendingRequests.delete(requestId);
                }
            });
        } catch (err) {
            clearTimeout(timeout);
            res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('502 Bad Gateway: خطأ غير متوقع في قناة الاتصال.');
            pendingRequests.delete(requestId);
        }
    });
});

// 2. دمج الـ WebSocketServer
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

wss.on('connection', (ws) => {
    console.log('🟩 تم ربط جهازك المحلي بالنفق بنجاح!');
    activeClient = ws;
    ws.isAlive = true;

    // 🌟 نظام نبضات القلب (Heartbeat): للتأكد التام أن الاتصال حقيقي وليس وهمياً
    const pingInterval = setInterval(() => {
        if (ws.isAlive === false) {
            console.log('🟥 العميل لم يستجب للـ Ping. يتم إغلاق الاتصال الميت فوراً.');
            return ws.terminate();
        }
        ws.isAlive = false;
        try { ws.ping(); } catch (e) { ws.terminate(); }
    }, 30000); // يفحص كل 30 ثانية

    ws.on('pong', () => {
        ws.isAlive = true; // العميل رد بأنه حي، الاتصال سليم!
    });

    ws.on('message', (message) => {
        try {
            const responseData = JSON.parse(message.toString());
            const pending = pendingRequests.get(responseData.id);

            if (pending) {
                clearTimeout(pending.timeout); // إلغاء التايم آوت فوراً
                
                const headers = responseData.headers || {};
                delete headers['connection'];
                delete headers['transfer-encoding'];

                pending.res.writeHead(responseData.status || 200, headers);
                const bodyBuffer = Buffer.from(responseData.body || '', 'base64');
                pending.res.end(bodyBuffer);
                
                pendingRequests.delete(responseData.id);
            }
        } catch (err) {
            console.error('❌ خطأ أثناء معالجة رد الكلاينت:', err);
        }
    });

    ws.on('close', () => {
        console.log('🟥 انقطع اتصال الجهاز المحلي بالنفق.');
        clearInterval(pingInterval);
        if (activeClient === ws) activeClient = null;
    });

    ws.on('error', (err) => {
        console.error('🚨 خطأ في سوكت العميل:', err.message);
        ws.terminate();
    });
});

server.listen(PORT, () => {
    console.log(`🚀 سيرفر النفق يعمل بنجاح على بورت ${PORT}`);
});
