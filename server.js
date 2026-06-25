const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

// 🛡️ درع حماية شامل للسيرفر
process.on('uncaughtException', (err) => {
    console.error('🔥 [حماية] السيرفر مستمر رغم الخطأ:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('🔥 [unhandledRejection]:', err);
});

// ⚙️ إعدادات قابلة للتعديل عبر متغيرات البيئة
const PORT = process.env.PORT || 3000;
const AUTH_TOKEN = process.env.AUTH_TOKEN || ''; // ضع توكن سري هنا لتفعيل المصادقة
const REQUEST_TIMEOUT_MS = 30000;       // ⏱️ timeout لكل طلب HTTP معلّق
const HEARTBEAT_INTERVAL_MS = 15000;    // 💓 فحص الاتصال كل 15 ثانية
const HEARTBEAT_TIMEOUT_MS = 10000;     // 💀 إذا لم يصل pong خلال 10 ثوانٍ = الاتصال ميت

// 🗂️ حالة الاتصال الحالي — كل اتصال له ID فريد لتفادي race condition
let activeConnection = null;            // { id, ws, pending: Map<reqId, {res, timer}> }

const server = http.createServer((req, res) => {
    // 🔒 التحقق من وجود اتصال حي
    if (!activeConnection || activeConnection.ws.readyState !== WebSocket.OPEN) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('502 Bad Gateway: الجهاز المحلي غير متصل.');
    }

    const conn = activeConnection;          // التقط لقطة للاتصال الحالي
    const reqId = crypto.randomUUID();
    const bodyChunks = [];

    req.on('data', chunk => bodyChunks.push(chunk));

    req.on('end', () => {
        // 🛡️ تحقق مجدداً أن الاتصال ما زال نفسه (لم يُستبدل بأحدث)
        if (activeConnection !== conn || conn.ws.readyState !== WebSocket.OPEN) {
            if (!res.writableEnded) {
                res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                res.end('502 Bad Gateway: انقطع الاتصال أثناء استلام الطلب.');
            }
            return;
        }

        const requestData = {
            id: reqId,
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: Buffer.concat(bodyChunks).toString('base64'),
            isBodyBase64: true
        };

        // ⏱️ timer لقتل الطلب المعلّق تلقائياً (يمنع 504 للأبد)
        const timer = setTimeout(() => {
            const pending = conn.pending.get(reqId);
            if (pending && !pending.res.writableEnded) {
                console.error(`⏱️ [timeout] الطلب ${reqId} لم يصل رده خلال ${REQUEST_TIMEOUT_MS}ms`);
                pending.res.writeHead(504, { 'Content-Type': 'text/plain; charset=utf-8' });
                pending.res.end('504 Gateway Timeout: الكلاينت المحلي لم يرد في الوقت المحدد.');
            }
            conn.pending.delete(reqId);
        }, REQUEST_TIMEOUT_MS);

        conn.pending.set(reqId, { res, timer });

        // 📤 إرسال الطلب للكلاينت المحلي مع callback للتحقق من نجاح الإرسال
        conn.ws.send(JSON.stringify(requestData), (sendErr) => {
            if (sendErr) {
                console.error(`📤 [send error] فشل إرسال الطلب ${reqId}:`, sendErr.message);
                const pending = conn.pending.get(reqId);
                if (pending && !pending.res.writableEnded) {
                    clearTimeout(pending.timer);
                    pending.res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
                    pending.res.end('502 Bad Gateway: فشل إرسال الطلب للكلاينت المحلي.');
                }
                conn.pending.delete(reqId);
            }
        });
    });

    // 🌟 لو المتصفح ألغى الطلب (أغلق الاتصال فعلياً قبل اكتمال الرد)
    // ملاحظة هامة: في Node.js الحديث، 'close' على req يُطلق بعد end مباشرة، لذا نستخدم res.on('close')
    res.on('close', () => {
        // res.on('close') يُطلق فعلياً عند:
        //   - إغلاق المتصفح للاتصال قبل اكتمال الرد (هذا ما نريد التقاطه)
        //   - بعد res.end() بنجاح (لكن وقتها writableEnded = true فلن ندخل الشرط)
        if (!res.writableEnded && conn.pending) {
            const pending = conn.pending.get(reqId);
            if (pending) {
                clearTimeout(pending.timer);
                conn.pending.delete(reqId);
            }
        }
    });
});

// 🔄 ترقية WebSocket مع مصادحة اختيارية بالتوكن
const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    // 🔒 مصادحة بسيطة بالتوكن إن كان مفعّلاً
    if (AUTH_TOKEN) {
        const url = new URL(request.url, 'http://localhost');
        const token = url.searchParams.get('token') || request.headers['x-proxy-token'];
        if (token !== AUTH_TOKEN) {
            socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            socket.destroy();
            return;
        }
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
        const connId = crypto.randomUUID();
        const conn = {
            id: connId,
            ws: ws,
            pending: new Map(),
            isAlive: true,
            heartbeatTimer: null,
            heartbeatInterval: null
        };

        // 🚨 هام: استبدال الاتصال القديم بأمان (دون أن يمسح القديم حالة الجديد)
        if (activeConnection && activeConnection.ws.readyState === WebSocket.OPEN) {
            console.log('🔄 اتصال جديد وصل — استبدال القديم...');
            // قتل heartbeat القديم قبل الاستبدال
            if (activeConnection.heartbeatInterval) clearInterval(activeConnection.heartbeatInterval);
            if (activeConnection.heartbeatTimer) clearTimeout(activeConnection.heartbeatTimer);
            // فشل طلبات القديم بسرعة
            failAllPending(activeConnection, 502, '502 Bad Gateway: تم استبدال الاتصال بآخر جديد.');
            // إغلاق القديم بصمت
            try { activeConnection.ws.removeAllListeners('close'); activeConnection.ws.close(); } catch (e) {}
        }

        activeConnection = conn;
        console.log(`🟩 [${connId.slice(0,8)}] تم ربط الكلاينت المحلي بنجاح!`);

        // 💓 نظام Heartbeat: اكتشاف الاتصالات النصف ميتة (Half-open)
        conn.heartbeatInterval = setInterval(() => {
            if (conn.ws.readyState !== WebSocket.OPEN) return;
            if (!conn.isAlive) {
                console.log(`💀 [${connId.slice(0,8)}] لم يصل pong — الاتصال ميت، إغلاق قسري.`);
                conn.ws.terminate();
                return;
            }
            conn.isAlive = false;
            try {
                conn.ws.ping();
            } catch (e) {
                console.error('ping error:', e.message);
            }
        }, HEARTBEAT_INTERVAL_MS);

        ws.on('pong', () => {
            conn.isAlive = true;
        });

        ws.on('message', (message) => {
            try {
                const responseData = JSON.parse(message.toString());
                const pending = conn.pending.get(responseData.id);
                if (!pending) {
                    // الطلب تم إلغاؤه أو انتهى timeout بالفعل — تجاهل
                    return;
                }
                if (!pending.res.writableEnded) {
                    let finalBody = responseData.body || '';
                    if (responseData.isBase64 && responseData.body) {
                        finalBody = Buffer.from(responseData.body, 'base64');
                    }
                    clearTimeout(pending.timer);
                    pending.res.writeHead(responseData.status, responseData.headers);
                    pending.res.end(finalBody);
                }
                conn.pending.delete(responseData.id);
            } catch (e) {
                console.error('خطأ في معالجة الرد:', e.message);
            }
        });

        ws.on('close', () => {
            console.log(`🟥 [${connId.slice(0,8)}] الكلاينت فصل الاتصال. تفريغ الطلبات المعلقة...`);
            // 🛡️ فقط امسح activeConnection لو هو نفسه — تفادي race condition
            if (activeConnection === conn) {
                activeConnection = null;
            }
            if (conn.heartbeatInterval) clearInterval(conn.heartbeatInterval);
            if (conn.heartbeatTimer) clearTimeout(conn.heartbeatTimer);
            failAllPending(conn, 502, '502 Bad Gateway: انقطع الاتصال بالكلاينت المحلي فجأة.');
        });

        ws.on('error', (err) => {
            console.error(`⚠️ [${connId.slice(0,8)}] ws error:`, err.message);
        });
    });
});

// 🛠️ دالة مساعدة لفشل كل الطلبات المعلقة لاتصال معيّن
function failAllPending(conn, status, message) {
    if (!conn || !conn.pending) return;
    for (const [id, pending] of conn.pending.entries()) {
        if (pending.timer) clearTimeout(pending.timer);
        if (!pending.res.headersSent && !pending.res.writableEnded) {
            try {
                pending.res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8' });
                pending.res.end(message);
            } catch (e) {}
        }
    }
    conn.pending.clear();
}

// 📊 تقرير دوري عن الحالة (للمراقبة)
setInterval(() => {
    const pendingCount = activeConnection ? activeConnection.pending.size : 0;
    const connState = activeConnection
        ? (activeConnection.ws.readyState === WebSocket.OPEN ? 'OPEN' : 'CLOSING/CLOSED')
        : 'لا اتصال';
    console.log(`📊 [status] اتصال: ${connState} | طلبات معلقة: ${pendingCount}`);
}, 30000);

server.listen(PORT, () => {
    console.log(`🚀 Proxy running on port ${PORT}`);
    console.log(`   ⏱️  request timeout: ${REQUEST_TIMEOUT_MS}ms`);
    console.log(`   💓 heartbeat: كل ${HEARTBEAT_INTERVAL_MS}ms`);
    if (AUTH_TOKEN) console.log(`   🔒 المصادقة بالتوكن مفعّلة`);
});
