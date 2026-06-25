const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

const server = http.createServer((req, res) => {
    // التحقق من وجود اتصال كلاينت
    if (!wss.clients.size) {
        res.writeHead(502);
        return res.end('Client Not Connected');
    }

    const id = crypto.randomUUID();
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
        const client = [...wss.clients][0];
        client.send(JSON.stringify({
            id, method: req.method, url: req.url, headers: req.headers,
            body: Buffer.concat(chunks).toString('base64')
        }));
        pendingRequests.set(id, res);
    });
});

const wss = new WebSocket.Server({ server });
const pendingRequests = new Map();

wss.on('connection', (ws) => {
    console.log('🔗 Client Connected');
    ws.on('message', (msg) => {
        const resData = JSON.parse(msg);
        const res = pendingRequests.get(resData.id);
        if (res) {
            res.writeHead(resData.status, resData.headers);
            res.end(Buffer.from(resData.body, 'base64'));
            pendingRequests.delete(resData.id);
        }
    });
});

// تفعيل Ping لمنع Render من غلق الاتصال
setInterval(() => {
    wss.clients.forEach(ws => ws.ping());
}, 25000);

server.listen(process.env.PORT || 3000);
