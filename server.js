const http = require('http');
const WebSocket = require('ws');

const pendingRequests = new Map(); // لتخزين الطلبات المعلقة ومنع تداخلها
let localClientSocket = null;

const server = http.createServer((req, res) => {
    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain' });
        return res.end('Bad Gateway: Local machine is offline.');
    }

    // توليد معرف فريد لكل طلب لمنع الـ 502 والتداخل
    const requestId = Math.random().toString(36).substring(2, 15);

    let bodyChunks = [];
    req.on('data', chunk => bodyChunks.push(chunk));
    req.on('end', () => {
        const requestData = {
            id: requestId, 
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: Buffer.concat(bodyChunks).toString('base64'),
            isBodyBase64: true
        };

        pendingRequests.set(requestId, res);
        localClientSocket.send(JSON.stringify(requestData));

        // حماية: إذا تأخر الرد أكثر من 30 ثانية يتم إلغاؤه تلقائيًا
        setTimeout(() => {
            if (pendingRequests.has(requestId)) {
                const pendingRes = pendingRequests.get(requestId);
                pendingRes.writeHead(504, { 'Content-Type': 'text/plain' });
                pendingRes.end('Gateway Timeout');
                pendingRequests.delete(requestId);
            }
        }, 30000);
    });
});

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        localClientSocket = ws;
        
        ws.on('message', (message) => {
            try {
                const responseData = JSON.parse(message);
                const pendingRes = pendingRequests.get(responseData.id);

                if (pendingRes) {
                    let finalBody = responseData.body;
                    if (responseData.isBase64 && responseData.body) {
                        finalBody = Buffer.from(responseData.body, 'base64');
                    }
                    res.writeHead(responseData.status, responseData.headers); // تصحيح الاستجابة
                    pendingRes.writeHead(responseData.status, responseData.headers);
                    pendingRes.end(finalBody);
                    pendingRequests.delete(responseData.id); // تنظيف فوري
                }
            } catch (e) {}
        });

        ws.on('close', () => { localClientSocket = null; });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`🚀 Proxy running on port ${PORT}`); });
