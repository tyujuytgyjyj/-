const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto'); // لاستخراج ID فريد لكل طلب

const pendingRequests = new Map(); // خزانة لحفظ الطلبات
let localClientSocket = null;

const server = http.createServer((req, res) => {
    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: الجهاز المحلي غير متصل.');
    }

    // 🌟 إنشاء ID فريد لهذا الطلب بالذات
    const reqId = crypto.randomUUID();

    let bodyChunks = [];
    req.on('data', chunk => bodyChunks.push(chunk));
    req.on('end', () => {
        const requestData = {
            id: reqId, // إرسال الـ ID للعميل
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: Buffer.concat(bodyChunks).toString('base64'),
            isBodyBase64: true
        };

        // حفظ الطلب في الخزانة لحين عودة الرد من جهازك
        pendingRequests.set(reqId, res);
        localClientSocket.send(JSON.stringify(requestData));
    });
});

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        localClientSocket = ws;
        console.log('🟩 تم ربط الكلاينت المحلي بنجاح!');

        // 🌟 مستمع واحد فقط لكل الرسائل (يفرزها حسب الـ ID)
        ws.on('message', (message) => {
            try {
                const responseData = JSON.parse(message.toString());
                const originalRes = pendingRequests.get(responseData.id); // البحث عن صاحب الطلب

                if (originalRes) {
                    let finalBody = responseData.body || '';
                    if (responseData.isBase64 && responseData.body) {
                        finalBody = Buffer.from(responseData.body, 'base64');
                    }

                    // إرسال الرد للمتصفح
                    originalRes.writeHead(responseData.status, responseData.headers);
                    originalRes.end(finalBody);
                    
                    // حذف الطلب من الخزانة بعد نجاحه
                    pendingRequests.delete(responseData.id);
                }
            } catch (e) {
                console.error('خطأ في معالجة الرد:', e.message);
            }
        });

        ws.on('close', () => {
            localClientSocket = null;
            pendingRequests.clear();
        });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`🚀 Proxy running on port ${PORT}`); });
