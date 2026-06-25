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
            const responseData = JSON.parse(message);
            res.writeHead(responseData.status, responseData.headers);
            res.end(responseData.body);
            localClientSocket.off('message', responseHandler);
        };
        localClientSocket.on('message', responseHandler);
    });
});

const wss = new WebSocket.Server({ noServer: true });
let localClientSocket = null;

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        console.log('⚡ جهازك المحلي اتصل بالنفق بنجاح!');
        localClientSocket = ws;
        
        ws.on('close', () => {
            console.log('❌ انقطع اتصال الجهاز المحلي.');
            localClientSocket = null;
        });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Proxy running on port ${PORT}`);
});
