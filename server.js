const http = require('http');
const WebSocket = require('ws');

const pendingRequests = new Map();
let localClientSocket = null;

const server = http.createServer((req, res) => {
    // فحص الصحة القياسي لـ Render ليبقى السيرفر Live دائماً في المتصفح
    if (req.url === '/' || req.url === '/health' || req.url === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('🚀 سيرفر النفق يعمل بنجاح ومستعد لاستقبال الاتصالات.');
    }

    // التحقق من أن جهازك متصل وموثق بالكامل داخل السيرفر
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
        
        // إرسال الطلب الخارجي مغلفاً لجهازك المحلي
        localClientSocket.send(JSON.stringify({ type: 'request', data: requestData }));

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

// 🌟 تمرير ترقية الاتصال بسلاسة ودون قيود لتجنب أخطاء الـ 401 من Render
server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.isAuthenticated = false; // افتراضياً الاتصال غير موثق حتى يثبت العكس

    // مهلة أمان: إذا لم يرسل الكلاينت التوكن الصحيح خلال 5 ثوانٍ يتم تدمير الاتصال فوراً
    const authTimeout = setTimeout(() => {
        if (!ws.isAuthenticated) {
            console.log('⚠️ تم طرد اتصال مجهول لم يقم بالتوثيق خلال المهلة المحددة.');
            ws.terminate();
        }
    }, 5000);

    ws.on('pong', () => ws.isAlive = true);

    ws.on('message', (message) => {
        try {
            const parsed = JSON.parse(message.toString());

            // 🌟 مرحلة الفحص والتعارف الداخلي
            if (!ws.isAuthenticated) {
                if (parsed.type === 'auth' && parsed.token === 'my-super-secret-token-123') {
                    ws.isAuthenticated = true;
                    clearTimeout(authTimeout); // إيقاف مؤقت الطرد
                    
                    if (localClientSocket && localClientSocket.readyState === WebSocket.OPEN) {
                        localClientSocket.close();
                    }
                    localClientSocket = ws;
                    console.log('⚡ تم توثيق اتصال جهازك المحلي بنجاح واستقرار عالي!');
                    ws.send(JSON.stringify({ type: 'auth_success' })); // إبلاغ الكلاينت بالنجاح
                    return;
                } else {
                    console.log('⚠️ توكن خاطئ! محاولة اختراق أو اتصال عشوائي وتم حظرها.');
                    ws.terminate();
                    return;
                }
            }

            // استقبال الردود القادمة من بورت 4000 بجهازك وتمريرها للمتصفح الخارجي
            if (parsed.id) {
                const res = pendingRequests.get(parsed.id);
                if (res) {
                    const resBuffer = Buffer.from(parsed.body, 'base64');
                    const cleanHeaders = { ...parsed.headers };
                    
                    delete cleanHeaders['transfer-encoding']; 
                    delete cleanHeaders['connection'];
                    cleanHeaders['content-length'] = resBuffer.length.toString();

                    res.writeHead(parsed.status, cleanHeaders);
                    res.end(resBuffer);
                    pendingRequests.delete(parsed.id);
                }
            }
        } catch (error) {
            console.error('خطأ في معالجة البيانات:', error.message);
        }
    });

    ws.on('close', () => {
        if (localClientSocket === ws) localClientSocket = null;
        if (ws.isAuthenticated) {
            pendingRequests.forEach((res) => {
                if (!res.headersSent) {
                    res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                    res.end('502 Bad Gateway: Connection Lost.');
                }
            });
            pendingRequests.clear();
        }
    });
});

setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Proxy running on port ${PORT}`));
