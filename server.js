const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain' });
        return res.end('Bad Gateway: Local machine is offline.');
    }

    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
        const requestData = {
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: body
        };

        localClientSocket.send(JSON.stringify(requestData));

        const responseHandler = (message) => {
            try {
                const responseData = JSON.parse(message);
                
                // فك التشفير الآمن والسريع للملفات والصفحات الكبيرة
                let finalBody = responseData.body;
                if (responseData.isBase64) {
                    finalBody = Buffer.from(responseData.body, 'base64');
                }

                res.writeHead(responseData.status, responseData.headers);
                res.end(finalBody);
                localClientSocket.off('message', responseHandler);
            } catch (e) {
                // تجنب التداخل في رسائل Socket.io
            }
        };
        localClientSocket.on('message', responseHandler);
    });
});

const wss = new WebSocket.Server({ 
    noServer: true,
    maxPayload: 50 * 1024 * 1024 
});
let localClientSocket = null;

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        localClientSocket = ws;
        ws.on('close', () => { localClientSocket = null; });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`🚀 Proxy running on port ${PORT}`); });
