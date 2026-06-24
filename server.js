const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
let activeTunnel = null;
const requestsMap = new Map();

const server = http.createServer((req, res) => {
    // إذا كان جهازك غير متصل بالنفق
    if (!activeTunnel || activeTunnel.readyState !== 1) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: النفق مغلق، شغل client.js في جهازك.');
    }

    const id = Math.random().toString(36).substring(2);
    const chunks = [];
    
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
        requestsMap.set(id, res);
        
        // إرسال الطلب لجهازك
        const payload = {
            id,
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: Buffer.concat(chunks).toString('base64')
        };
        
        try {
            activeTunnel.send(JSON.stringify(payload));
        } catch (e) {
            res.writeHead(502);
            res.end('Proxy Error');
            requestsMap.delete(id);
        }
    });
});

const wss = new WebSocketServer({ server });
wss.on('connection', (ws) => {
    console.log('🟩 Connected');
    activeTunnel = ws;

    ws.on('message', (message) => {
        try {
            const response = JSON.parse(message.toString());
            const originalRes = requestsMap.get(response.id);
            
            if (originalRes) {
                originalRes.writeHead(response.status, response.headers);
                originalRes.end(Buffer.from(response.body, 'base64'));
                requestsMap.delete(response.id);
            }
        } catch (e) {}
    });
});

server.listen(PORT);
