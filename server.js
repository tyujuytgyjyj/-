const http = require('http');
const WebSocket = require('ws');

const pendingRequests = new Map();
let localClientSocket = null;

const server = http.createServer((req, res) => {
    // 1. مسار الفحص القياسي لـ Render ليبقى السيرفر Live دائماً
    if (req.url === '/render-health-check' || req.url === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('🚀 Connected');
    }

    // 2. فحص بصمة الكوكيز للشاشة الترحيبية
    const cookies = req.headers.cookie || '';
    const hasPassedSplash = cookies.includes('hamida_passed=true');

    // إظهار شاشة الانتظار فقط عند الدخول لأول مرة على الرابط الرئيسي '/'
    if (req.url === '/' && !hasPassedSplash) {
        // زرع الكوكيز في متصفح الزائر وتنتهي صلاحيته بعد ساعة تلقائياً
        res.writeHead(200, {
            'Set-Cookie': 'hamida_passed=true; Path=/; Max-Age=3600;',
            'Content-Type': 'text/html; charset=utf-8'
        });
        return res.end(`
            <!DOCTYPE html>
            <html lang="ar" dir="rtl">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>انتظار التوجيه...</title>
                <style>
                    body {
                        background-color: #000000;
                        color: #ffffff;
                        font-family: sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        margin: 0;
                        font-size: 32px;
                        font-weight: bold;
                    }
                    .waiting-text { animation: pulse 1.5s infinite ease-in-out; }
                    @keyframes pulse { 0%, 100% { opacity: 0.5; } 50% { opacity: 1; } }
                </style>
            </head>
            <body>
                <div class="waiting-text">يا حميدة منتضر انتضر...</div>
                <script>
                    // الانتظار ثانيتين ثم تحديث الصفحة بشكل نظيف ودون التلاعب بالروابط
                    setTimeout(() => {
                        window.location.reload();
                    }, 2000);
                </script>
            </body>
            </html>
        `);
    }

    // 3. تمرير الطلب عبر النفق إذا كان الزائر يملك البصمة أو يطلب ملفات أخرى (CSS, JS, إلخ)
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
        
        try {
            localClientSocket.send(JSON.stringify(requestData));
        } catch (err) {
            res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('502 Bad Gateway: فشل إرسال البيانات للنفق.');
            pendingRequests.delete(reqId);
            return;
        }

        // مهلة أمان قصيرة (20 ثانية) لتجنب تعليق سيرفر Render وإعطاء خطأ مخصص ومفهوم
        setTimeout(() => {
            if (pendingRequests.has(reqId)) {
                const resObj = pendingRequests.get(reqId);
                if (!resObj.headersSent) {
                    resObj.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
                    resObj.end('504 Gateway Timeout: استغرق السيرفر المحلي وقتاً طويلاً جداً للاستجابة.');
                }
                pendingRequests.delete(reqId);
            }
        }, 20000);
    });
});

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        localClientSocket = ws;
        console.log('⚡ تم ربط الكلاينت المحلي بالنفق!');

        ws.on('message', (message) => {
            try {
                const responseData = JSON.parse(message.toString());
                if (!responseData.id) return;

                const res = pendingRequests.get(responseData.id);
                if (res) {
                    const resBuffer = Buffer.from(responseData.body || '', 'base64');
                    const cleanHeaders = { ...responseData.headers };
                    
                    delete cleanHeaders['transfer-encoding']; 
                    delete cleanHeaders['connection'];
                    cleanHeaders['content-length'] = resBuffer.length.toString();

                    res.writeHead(responseData.status || 200, cleanHeaders);
                    res.end(resBuffer);
                    pendingRequests.delete(responseData.id);
                }
            } catch (error) {
                console.error('خطأ في معالجة الرد:', error.message);
            }
        });

        ws.on('close', () => {
            if (localClientSocket === ws) {
                localClientSocket = null;
                // حماية فورية: تنظيف وإغلاق أي طلبات معلقة فور انقطاع اتصال الكمبيوتر لمنع الـ 502 الطويل
                pendingRequests.forEach((res) => {
                    if (!res.headersSent) {
                        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                        res.end('502 Bad Gateway: انقطع اتصال النفق فجأة.');
                    }
                });
                pendingRequests.clear();
            }
        });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Proxy running on port ${PORT}`));
