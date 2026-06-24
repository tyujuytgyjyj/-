const http = require('http');
const WebSocket = require('ws');

// خريطة لتخزين الطلبات المعلقة لربط كل رد بالزائر الصحيح
const pendingRequests = new Map();

const server = http.createServer((req, res) => {
    // 1. مسار فحص الصحة (مهم جداً لمنع Render من إعادة تشغيل السيرفر)
    if (req.url === '/health' || req.url === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        return res.end('OK');
    }

    // 2. التحقق من اتصال جهازك
    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain' });
        return res.end('Bad Gateway: Local machine is offline.');
    }

    // 3. تجميع الطلب وإرساله مع ID فريد
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
        // توليد معرف فريد لكل طلب لتجنب تداخل زوار الموقع
        const reqId = Date.now().toString(36) + Math.random().toString(36).substring(2);

        const requestData = {
            id: reqId,
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: body
        };

        // حفظ كائن الرد لنتمكن من إرسال البيانات له لاحقاً
        pendingRequests.set(reqId, res);
        localClientSocket.send(JSON.stringify(requestData));

        // مهلة زمنية (Timeout) لتجنب تعليق الطلب للأبد في حال لم يرد جهازك
        setTimeout(() => {
            if (pendingRequests.has(reqId)) {
                const resObj = pendingRequests.get(reqId);
                if (!resObj.headersSent) {
                    resObj.writeHead(504, { 'Content-Type': 'text/plain' });
                    resObj.end('Gateway Timeout: Local machine took too long to respond.');
                }
                pendingRequests.delete(reqId);
            }
        }, 30000); // 30 ثانية
    });
});

const wss = new WebSocket.Server({ noServer: true });
let localClientSocket = null;

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        console.log('⚡ جهازك المحلي اتصل بالنفق بنجاح!');
        
        // إغلاق أي اتصال قديم معلق لتجنب التعارض
        if (localClientSocket && localClientSocket.readyState === WebSocket.OPEN) {
            localClientSocket.close();
        }
        localClientSocket = ws;

        // استلام الردود من جهازك المحلي
        ws.on('message', (message) => {
            try {
                const responseData = JSON.parse(message);
                const res = pendingRequests.get(responseData.id); // جلب الزائر الخاص بهذا الرد
                
                if (res) {
                    res.writeHead(responseData.status, responseData.headers);
                    res.end(responseData.body);
                    pendingRequests.delete(responseData.id); // تنظيف الذاكرة
                }
            } catch (error) {
                console.error('خطأ في قراءة الرد:', error.message);
            }
        });

        ws.on('close', () => {
            console.log('❌ انقطع اتصال الجهاز المحلي.');
            localClientSocket = null;
            // إنهاء جميع الطلبات المعلقة بخطأ 502 للمستخدمين
            pendingRequests.forEach((res) => {
                if (!res.headersSent) {
                    res.writeHead(502, { 'Content-Type': 'text/plain' });
                    res.end('Bad Gateway: Local machine disconnected.');
                }
            });
            pendingRequests.clear();
        });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Proxy running on port ${PORT}`);
});
