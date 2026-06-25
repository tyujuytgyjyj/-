const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

// 🛡️ درع حماية للسيرفر
process.on('uncaughtException', (err) => {
    console.error('🔥 [حماية] السيرفر مستمر رغم الخطأ:', err.message);
});

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
        
        if (localClientSocket && localClientSocket.readyState === WebSocket.OPEN) {
            localClientSocket.send(JSON.stringify(requestData));
        }
    });

    // 🌟 حماية جديدة: لو المتصفح قفل الصفحة، احذف الطلب عشان السيرفر ميعلقش
    req.on('close', () => {
        if (!res.writableEnded) {
            pendingRequests.delete(reqId);
        }
    });
});

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        localClientSocket = ws;
        console.log('🟩 تم ربط الكلاينت المحلي بنجاح!');

        ws.on('message', (message) => {
            try {
                const responseData = JSON.parse(message.toString());
                const originalRes = pendingRequests.get(responseData.id); 

                if (originalRes && !originalRes.writableEnded) {
                    let finalBody = responseData.body || '';
                    if (responseData.isBase64 && responseData.body) {
                        finalBody = Buffer.from(responseData.body, 'base64');
                    }

                    originalRes.writeHead(responseData.status, responseData.headers);
                    originalRes.end(finalBody);
                    
                    pendingRequests.delete(responseData.id);
                }
            } catch (e) {
                console.error('خطأ في معالجة الرد:', e.message);
            }
        });

        ws.on('close', () => {
            console.log('🟥 الكلاينت فصل الاتصال. جاري تفريغ الطلبات المعلقة...');
            localClientSocket = null;
            
            for (const [id, res] of pendingRequests.entries()) {
                if (!res.headersSent && !res.writableEnded) {
                    res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                    res.end('502 Bad Gateway: انقطع الاتصال بالكلاينت المحلي فجأة.');
                }
            }
            pendingRequests.clear();
        });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`🚀 Proxy running on port ${PORT}`); });
