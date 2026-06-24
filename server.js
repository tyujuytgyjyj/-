const http = require('http');
const httpProxy = require('http-proxy');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;

// إنشاء الـ Proxy المفتوح المصدر
const proxy = httpProxy.createProxyServer({
    target: {
        host: '127.0.0.1',
        port: 4000 // البورت الافتراضي لمشروعك
    },
    ws: true
});

const server = http.createServer((req, res) => {
    // هنا يتم تمرير أي طلب قادم من الإنترنت تلقائياً إلى التفق
    if (activeTargetSocket && activeTargetSocket.readyState === 1) {
        // توجيه الطلب عبر البروكسي
        proxy.web(req, res, { target: 'http://127.0.0.1:4000' }, (err) => {
            res.writeHead(502);
            res.end('Reverse Proxy Error');
        });
    } else {
        res.writeHead(502);
        res.end('502 Bad Gateway: Local tunnel client is offline.');
    }
});

// فتح سوكت مخصص فقط لربط جهازك المحلي بالسيرفر
const wss = new WebSocketServer({ noServer: true });
let activeTargetSocket = null;

server.on('upgrade', (request, socket, head) => {
    if (request.url === '/tunnel-connect') {
        wss.handleUpgrade(request, socket, head, (ws) => {
            activeTargetSocket = ws;
            console.log('🟩 الجهاز المحلي متصل بالبروكسي بنجاح!');
        });
    } else {
        // إذا كان الطلب عبارة عن WebSocket عادي من مشروعك (بورت 4000)
        proxy.ws(request, socket, head);
    }
});

server.listen(PORT, () => console.log(`🚀 Open-Source Proxy running on port ${PORT}`));
