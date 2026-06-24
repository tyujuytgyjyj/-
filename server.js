const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;
const pendingRequests = new Map();
let activeClient = null;

// 1. إنشاء سيرفر HTTP لاستقبال طلبات الزوار
const server = http.createServer((req, res) => {
    // إذا كان جهازك المحلي غير متصل بالنفق، نرد فوراً بـ 502 بدل التعليق
    if (!activeClient || activeClient.readyState !== 1) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: جهازك المحلي غير متصل بالنفق حالياً.');
    }

    const requestId = crypto.randomUUID(); // توليد ID فريد لكل طلب
    const chunks = [];

    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
        const bodyBuffer = Buffer.concat(chunks);
        
        const requestData = {
            id: requestId,
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: bodyBuffer.toString('base64') // تحويل البودي لـ base64 لحمايته من التلف
        };

        // حماية قصوى: إذا لم يستجب جهازك خلال 20 ثانية، ننهي الطلب لمنع الـ 504
        const timeout = setTimeout(() => {
            if (pendingRequests.has(requestId)) {
                const pending = pendingRequests.get(requestId);
                pending.res.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
                pending.res.end('504 Gateway Timeout: السيرفر المحلي استغرق وقتاً طويلاً في الاستجابة.');
                pendingRequests.delete(requestId);
            }
        }, 20000);

        // حفظ استجابة الـ HTTP في الذاكرة لربطها لاحقاً برد الـ WebSocket
        pendingRequests.set(requestId, { res, timeout });
        
        // إرسال الطلب فوراً لجهازك عبر النفق
        activeClient.send(JSON.stringify(requestData));
    });
});

// 2. دمج سيرفر الـ WebSocket مع سيرفر الـ HTTP
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
    });
});

wss.on('connection', (ws) => {
    console.log('🟩 تم ربط جهازك المحلي بالنفق بنجاح!');
    activeClient = ws;

    ws.on('message', (message) => {
        try {
            const responseData = JSON.parse(message.toString());
            const pending = pendingRequests.get(responseData.id);

            if (pending) {
                clearTimeout(pending.timeout); // إلغاء التايم آوت فوراً لوصول الرد
                
                const headers = responseData.headers || {};
                // تنظيف الهيدرز لمنع تعليق المتصفح
                delete headers['connection'];
                delete headers['transfer-encoding'];

                pending.res.writeHead(responseData.status || 200, headers);
                
                // تحويل الرد القادم من جهازك من base64 إلى ملفات أصيلة (صور، نصوص، كود)
                const bodyBuffer = Buffer.from(responseData.body || '', 'base64');
                pending.res.end(bodyBuffer);
                
                pendingRequests.delete(responseData.id);
            }
        } catch (err) {
            console.error('❌ خطأ أثناء معالجة رد الكلاينت:', err);
        }
    });

    ws.on('close', () => {
        console.log('🟥 انقطع اتصال الجهاز المحلي بالنفق.');
        if (activeClient === ws) activeClient = null;
    });
});

server.listen(PORT, () => {
    console.log(`🚀 سيرفر النفق يعمل بنجاح على بورت ${PORT}`);
});
