const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;
const pendingRequests = new Map();
let activeClient = null;

const server = http.createServer((req, res) => {
    // حماية: إذا حاول متصفح دخول مسار النفق بالخطأ نمنعه
    if (req.url === '/tunnel-secure') {
        res.writeHead(400);
        return res.end('WS connection required');
    }

    // إذا كان النفق مغلقاً أو العميل غير متصل
    if (!activeClient || activeClient.readyState !== 1) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: النفق مغلق تماماً، تأكد من تشغيل ملف client.js الجديد في جهازك.');
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

        const timeout = setTimeout(() => {
            if (pendingRequests.has(requestId)) {
                const pending = pendingRequests.get(requestId);
                try {
                    pending.res.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
                    pending.res.end('504 Gateway Timeout: المشروع المحلي على بورت 4000 لم يستجب.');
                } catch(e){}
                pendingRequests.delete(requestId);
            }
        }, 15000);

        pendingRequests.set(requestId, { res, timeout });
        try {
            activeClient.send(JSON.stringify(requestData));
        } catch (err) {
            clearTimeout(timeout);
            res.writeHead(502);
            res.end('502 Bad Gateway: انقطع الاتصال بالنفق.');
            pendingRequests.delete(requestId);
        }
    });
});

// إعداد سوكت معزول وخاص بطلب الـ Upgrade يدوياً ليتوافق 100% مع بيئة Render
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    if (request.url === '/tunnel-secure') {
        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit('connection', ws, request);
        });
    } else {
        socket.destroy();
    }
});

wss.on('connection', (ws) => {
    console.log('🟩 [Render] تم فتح النفق بنجاح واستقبال اتصال جهازك المحلي.');
    activeClient = ws;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message.toString());
            
            // الرد الفوري على نبضات القلب القادمة من جهازك لتبقي الخط مفتوحاً رغماً عن جدار الحماية
            if (data.type === 'ping') {
                ws.send(JSON.stringify({ type: 'pong' }));
                return;
            }

            const pending = pendingRequests.get(data.id);
            if (pending) {
                clearTimeout(pending.timeout);
                const headers = data.headers || {};
                delete headers['connection'];
                delete headers['transfer-encoding'];
                pending.res.writeHead(data.status || 200, headers);
                pending.res.end(Buffer.from(data.body || '', 'base64'));
                pendingRequests.delete(data.id);
            }
        } catch (err) {}
    });

    ws.on('close', () => {
        if (activeClient === ws) activeClient = null;
    });
});

process.on('uncaughtException', (err) => console.error('Global Error:', err.message));

server.listen(PORT, () => console.log(`🚀 Tunnel Server running on port ${PORT}`));
