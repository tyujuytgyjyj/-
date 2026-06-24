const http = require('http');
const PORT = process.env.PORT || 10000;

let queue = [];
let responses = new Map();

const server = http.createServer((req, res) => {
    // 1. نقطة اتصال خاصة بجهازك المحلي لسحب طلبات الزوار
    if (req.url === '/get-local-reqs') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(queue));
        queue = []; // تفريغ الطابور بعد السحب
        return;
    }

    // 2. نقطة اتصال خاصة بجهازك المحلي لتسليم الردود
    if (req.url.startsWith('/submit-local-res/')) {
        const reqId = req.url.split('/').pop();
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            const data = JSON.parse(body);
            const originalRes = responses.get(reqId);
            if (originalRes) {
                originalRes.writeHead(data.status, data.headers);
                originalRes.end(Buffer.from(data.body, 'base64'));
                responses.delete(reqId);
            }
            res.writeHead(200);
            res.end('OK');
        });
        return;
    }

    // 3. استقبال طلبات الزوار العاديين وحفظها في الطابور
    const reqId = Math.random().toString(36).substring(2);
    let reqBody = '';
    req.on('data', chunk => reqBody += chunk);
    req.on('end', () => {
        queue.push({
            id: reqId,
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: Buffer.from(reqBody).toString('base64')
        });
        responses.set(reqId, res);

        // حماية من التعليق: إذا لم يسحب جهازك الطلب خلال 10 ثوانٍ نغلقه بـ 504
        setTimeout(() => {
            if (responses.has(reqId)) {
                responses.get(reqId).writeHead(504, { 'Content-Type': 'text/plain' });
                responses.get(reqId).end('504 Timeout: Local client did not poll in time.');
                responses.delete(reqId);
            }
        }, 10000);
    });
});

server.listen(PORT, () => console.log(`Proxy server running on port ${PORT}`));
