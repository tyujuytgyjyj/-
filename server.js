const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;
const pendingRequests = new Map();
let activeClient = null;

const server = http.createServer((req, res) => {
    if (!activeClient || activeClient.readyState !== 1) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: النفق مغلق حالياً أو الكلاينت يفصل ويصل.');
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
                    pending.res.end('504 Gateway Timeout: السيرفر المحلي لم يستجب.');
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
            res.end('502 Bad Gateway');
            pendingRequests.delete(requestId);
        }
    });
});

const wss = new WebSocketServer({ server });
wss.on('connection', (ws) => {
    console.log('🟩 جهاز العميل اتصل بالنفق!');
    activeClient = ws;

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message.toString());
            
            // 🌟 تجاهل رسالة نبضات القلب القادمة من جهازك (فقط للحفاظ على الاتصال حياً)
            if (data.type === 'heartbeat') return;

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

server.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
