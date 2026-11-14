// openGym relay (Render): public HTTPS front door <-> WebSocket <-> agent on the VM -> openGym.
// Agent connects to /__agent?key=<RELAY_KEY>. Everything else is relayed verbatim.
const http = require('http');
const { WebSocketServer } = require('ws');

const KEY = process.env.RELAY_KEY;
const PORT = process.env.PORT || 10000;
const CHUNK_TIMEOUT_MS = 90000;

let agent = null;
const pending = new Map(); // id -> {res, chunks, status, headers, done}

const server = http.createServer((req, res) => {
  if (req.url === '/__relay_health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('relay up'); return; }
  if (!agent || agent.readyState !== 1) { res.writeHead(502, { 'content-type': 'text/plain' }); res.end('openGym server is offline right now.'); return; }
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => {
    const id = Math.random().toString(36).slice(2) + Date.now().toString(36);
    const body = Buffer.concat(chunks);
    const entry = { res, chunks: [], status: 200, headers: {}, timer: null };
    entry.timer = setTimeout(() => {
      if (pending.has(id)) { pending.delete(id); try { res.writeHead(504); res.end('relay timeout'); } catch {} }
    }, CHUNK_TIMEOUT_MS);
    pending.set(id, entry);
    const headers = { ...req.headers };
    delete headers.host;
    try {
      agent.send(JSON.stringify({ t: 'req', id, method: req.method, path: req.url, headers, bodyB64: body.length ? body.toString('base64') : null }));
    } catch {
      pending.delete(id); clearTimeout(entry.timer);
      res.writeHead(502); res.end('relay send failed');
    }
  });
});

const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname !== '/__agent' || !KEY || u.searchParams.get('key') !== KEY) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => {
    if (agent) { try { agent.close(4000, 'replaced'); } catch {} }
    agent = ws;
    console.log('agent connected');
    ws.on('message', (data) => {
      let msg; try { msg = JSON.parse(data.toString()); } catch { return; }
      const p = pending.get(msg.id);
      if (!p) return;
      if (msg.t === 'res-start') { p.status = msg.status; p.headers = msg.headers || {}; }
      else if (msg.t === 'res-chunk') p.chunks.push(Buffer.from(msg.data, 'base64'));
      else if (msg.t === 'res-end') {
        pending.delete(msg.id); clearTimeout(p.timer);
        const h = { ...p.headers }; delete h['content-length']; delete h['transfer-encoding'];
        try { p.res.writeHead(p.status, h); p.res.end(Buffer.concat(p.chunks)); } catch {}
      } else if (msg.t === 'res-error') {
        pending.delete(msg.id); clearTimeout(p.timer);
        try { p.res.writeHead(502); p.res.end('relay error'); } catch {}
      }
    });
    const clear = () => { if (agent === ws) agent = null; };
    ws.on('close', clear);
    ws.on('error', clear);
  });
});

server.listen(PORT, () => console.log('relay listening on', PORT));
