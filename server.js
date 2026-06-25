const http = require('http');
const WebSocket = require('ws');

const server = http.createServer((req, res) => {
    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain' });
        return res.end('Bad Gateway: Local machine is offline.');
    }

    // تجميع الـ Body كـ Buffer لضمان وصول بيانات الـ POST (مثل تسجيل الدخول) سليمة 100%
    let bodyChunks = [];
    req.on('data', chunk => bodyChunks.push(chunk));
    req.on('end', () => {
        const requestData = {
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: Buffer.concat(bodyChunks).toString('base64'), // تشفير آمن لنقل البيانات
            isBodyBase64: true
        };

        localClientSocket.send(JSON.stringify(requestData));

        const responseHandler = (message) => {
            try {
                const responseData = JSON.parse(message);
                
                // فك تشفير الرد
                let finalBody = responseData.body;
                if (responseData.isBase64 && responseData.body) {
                    finalBody = Buffer.from(responseData.body, 'base64');
                }

                // تمرير الـ Headers والـ Cookies القادمة من السيرفر المحلي للمتصفح بدقة
                res.writeHead(responseData.status, responseData.headers);
                res.end(finalBody);
                localClientSocket.off('message', responseHandler);
            } catch (e) {
                // تجنب التداخل
            }
        };
        localClientSocket.on('message', responseHandler);
    });
});

const wss = new WebSocket.Server({ noServer: true });
let localClientSocket = null;

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        localClientSocket = ws;
        ws.on('close', () => { localClientSocket = null; });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`🚀 Proxy running on port ${PORT}`); });
