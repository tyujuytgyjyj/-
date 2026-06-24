const http = require('http');
const WebSocket = require('ws');

const pendingRequests = new Map();

const server = http.createServer((req, res) => {
    // 🌟 حل مشكلة فحص الصحة: إرجاع كود 200 للمسارات القياسية دائماً
    // هذا يجعل Render يرى السيرفر شغالاً تماماً (Healthy) ولا يحظر اتصالاتك
    if (req.url === '/' || req.url === '/health' || req.url === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('🚀 سيرفر النفق يعمل بنجاح ومستعد لاستقبال الاتصالات.');
    }

    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: الجهاز المحلي غير متصل بالنفق حالياً.');
    }

    let chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
        const reqId = Date.now().toString(36) + Math.random().toString(36).substring(2);
        const bodyBuffer = Buffer.concat(chunks);

        const requestData = {
            id: reqId,
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: bodyBuffer.toString('base64')
        };

        pendingRequests.set(reqId, res);
        localClientSocket.send(JSON.stringify(requestData));

        setTimeout(() => {
            if (pendingRequests.has(reqId)) {
                const resObj = pendingRequests.get(reqId);
                if (!resObj.headersSent) {
                    resObj.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
                    resObj.end('504 Gateway Timeout: استغرق الجهاز المحلي وقتاً طويلاً للاستجابة.');
                }
                pendingRequests.delete(reqId);
            }
        }, 30000);
    });
});

const wss = new WebSocket.Server({ noServer: true });
let localClientSocket = null;

setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

server.on('upgrade', (request, socket, head) => {
    // 🌟 جعل التحقق مرناً باستخدام startsWith لمنع الـ 502 الناتجة عن اختلاف صياغة الرابط
    if (request.url && request.url.startsWith('/_tunnel')) {
        wss.handleUpgrade(request, socket, head, (ws) => {
            console.log('⚡ جهازك المحلي اتصل بالنفق بنجاح!');
            
            if (localClientSocket && localClientSocket.readyState === WebSocket.OPEN) {
                localClientSocket.close();
            }
            
            localClientSocket = ws;
            ws.isAlive = true;

            ws.on('pong', () => ws.isAlive = true);

            ws.on('message', (message) => {
                try {
                    const responseData = JSON.parse(message.toString());
                    const res = pendingRequests.get(responseData.id);
                    
                    if (res) {
                        const resBuffer = Buffer.from(responseData.body, 'base64');
                        const cleanHeaders = { ...responseData.headers };
                        
                        delete cleanHeaders['transfer-encoding']; 
                        delete cleanHeaders['connection'];
                        cleanHeaders['content-length'] = resBuffer.length.toString();

                        res.writeHead(responseData.status, cleanHeaders);
                        res.end(resBuffer);
                        pendingRequests.delete(responseData.id);
                    }
                } catch (error) {
                    console.error('خطأ في معالجة الرد:', error.message);
                }
            });

            ws.on('close', () => {
                if (localClientSocket === ws) localClientSocket = null;
                pendingRequests.forEach((res) => {
                    if (!res.headersSent) {
                        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                        res.end('502 Bad Gateway: Connection Lost.');
                    }
                });
                pendingRequests.clear();
            });
        });
    } else {
        // رفض نظيف وصريح لأي طلبات اتصال عشوائية من المتصفح
        socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
        socket.destroy();
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Proxy running on port ${PORT}`));
