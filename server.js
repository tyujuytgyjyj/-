const http = require('http');
const WebSocket = require('ws');

const pendingRequests = new Map();
let localClientSocket = null;

const server = http.createServer((req, res) => {
    // مسار الفحص الخاص بـ Render ليبقى السيرفر مستيقظاً و Live دائماً
    if (req.url === '/render-health-check' || req.url === '/ping') {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('🚀 سيرفر النفق مستيقظ ويعمل في الخلفية بنجاح.');
    }

    // 🌟 1. إذا دخل الشخص على الرابط الأساسي مباشرة (تظهر له رسالة الانتظار)
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        return res.end(`
            <!DOCTYPE html>
            <html lang="ar" dir="rtl">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>انتظار التوجيه...</title>
                <style>
                    body {
                        background-color: #000000; /* خلفية سوداء بالكامل */
                        color: #ffffff; /* كتابة بيضاء متناسقة */
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        margin: 0;
                        font-size: 32px;
                        font-weight: bold;
                    }
                    .waiting-text {
                        animation: pulse 1.5s infinite ease-in-out;
                    }
                    @keyframes pulse {
                        0% { opacity: 0.5; }
                        50% { opacity: 1; }
                        100% { opacity: 0.5; }
                    }
                </style>
            </head>
            <body>
                <div class="waiting-text">يا حميدة منتضر انتضر...</div>

                <script>
                    // 🌟 الانتظار لمدة ثانيتين (2000ms) ثم التوجيه للرابط السري الخاص بالنفق
                    setTimeout(() => {
                        window.location.href = '/?passed=true';
                    }, 2000);
                </script>
            </body>
            </html>
        `);
    }

    // 🌟 2. إذا جاء الطلب ويحمل العبارة السرية (يعني أنه انتظر ثانيتين بالفعل)
    if (req.url === '/?passed=true') {
        req.url = '/'; // نُعيد تسمية الرابط إلى '/' لكي يفهمه سيرفرك المحلي كصفحة رئيسية عادية دون أخطاء
    }

    // التحقق من اتصال جهازك بالنفق
    if (!localClientSocket || localClientSocket.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: السيرفر المحلي (جهازك) غير متصل بالنفق حالياً.');
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
                    resObj.end('504 Gateway Timeout');
                }
                pendingRequests.delete(reqId);
            }
        }, 30000);
    });
});

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
        localClientSocket = ws;
        console.log('⚡ تم ربط العميل المحلي بنجاح!');

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
        });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Proxy running on port ${PORT}`));
