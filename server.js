const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

// درع حماية للسيرفر
process.on('uncaughtException', (err) => console.error('🔥 [حماية السيرفر]:', err.message));

const pendingRequests = new Map();
let localClientSocket = null;

const server = http.createServer((req, res) => {
    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: الجهاز المحلي غير متصل.');
    }

    const reqId = crypto.randomUUID();
    let bodyChunks = [];
    
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

        pendingRequests.set(reqId, res);
        localClientSocket.send(JSON.stringify(requestData));
        
        // ⏳ حماية من التعليق: لو جهازك ماردش بعد 30 ثانية نقفل الطلب بـ 504
        setTimeout(() => {
            if (pendingRequests.has(reqId)) {
                const originalRes = pendingRequests.get(reqId);
                if (!originalRes.headersSent) {
                    originalRes.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
                    originalRes.end('504 Gateway Timeout: السيرفر المحلي تأخر في الرد.');
                }
                pendingRequests.delete(reqId);
            }
        }, 30000);
    });
});

// 🌟 السماح بمرور ملفات ضخمة تصل لـ 50 ميجا
const wss = new WebSocket.Server({ noServer: true, maxPayload: 50 * 1024 * 1024 });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        localClientSocket = ws;
        console.log('🟩 تم ربط الكلاينت المحلي بنجاح!');

        // 🌟 إعداد نظام النبض
        ws.isAlive = true;
        ws.on('pong', () => { ws.isAlive = true; });

        ws.on('message', (message) => {
            try {
                const responseData = JSON.parse(message.toString());
                const originalRes = pendingRequests.get(responseData.id);

                if (originalRes) {
                    let finalBody = responseData.body ? Buffer.from(responseData.body, 'base64') : '';
                    
                    if (!originalRes.headersSent) {
                        originalRes.writeHead(responseData.status, responseData.headers);
                        originalRes.end(finalBody);
                    }
                    pendingRequests.delete(responseData.id);
                }
            } catch (e) {
                console.error('خطأ في معالجة الرد:', e.message);
            }
        });

        ws.on('close', () => {
            console.log('🟥 الكلاينت فصل الاتصال.');
            localClientSocket = null;
            for (const [id, res] of pendingRequests.entries()) {
                if (!res.headersSent) {
                    res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                    res.end('502 Bad Gateway: انقطع الاتصال.');
                }
            }
            pendingRequests.clear();
        });
    });
});

// 🌟 فحص النبض كل 20 ثانية عشان ريندر ميفصلناش
setInterval(() => {
    if (localClientSocket) {
        if (localClientSocket.isAlive === false) return localClientSocket.terminate();
        localClientSocket.isAlive = false;
        localClientSocket.ping();
    }
}, 20000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`🚀 Proxy running on port ${PORT}`); });
