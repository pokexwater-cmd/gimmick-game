// Gimmick relay for Render: plain Node WebSocket server. Rooms live in memory, nothing is stored.
// Same wire format as the Cloudflare version:
//   "p{json}" presence meta  |  "m{json}" game message (relayed untouched)  |  "ping" -> "pong"
//   server -> client: {"k":"presence","state":{id:meta}}
const http = require('http');
const { WebSocketServer } = require('ws');

const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' }); res.end('gimmick-server ok'); });   // any GET works as a keep-alive ping
const wss = new WebSocketServer({ server, maxPayload: 60000 });
const rooms = new Map();   // room name -> Set of sockets

function state(set) { const o = {}; for (const w of set) if (w.meta) o[w.pid] = w.meta; return o; }
function presence(set) { const s = JSON.stringify({ k: 'presence', state: state(set) }); for (const w of set) if (w.readyState === 1) w.send(s); }

wss.on('connection', (ws, req) => {
  const u = new URL(req.url, 'http://x');
  const room = (u.searchParams.get('room') || 'lobby').slice(0, 64), id = (u.searchParams.get('id') || '').slice(0, 16);
  if (!id) return ws.close();
  let set = rooms.get(room); if (!set) rooms.set(room, set = new Set());
  if (room !== 'lobby' && ![...set].some(o => o.pid === id) && set.size >= 8) return ws.close();   // room cap (anti-flood)
  ws.bucket = 200; ws.bt = Date.now();
  for (const o of set) if (o.pid === id) { set.delete(o); try { o.close(); } catch (e) {} }   // same player reconnecting
  ws.pid = id; ws.meta = null; ws.isAlive = true; set.add(ws);
  ws.send(JSON.stringify({ k: 'presence', state: state(set) }));
  ws.on('message', (data, isBinary) => {
    const now = Date.now(); ws.bucket = Math.min(200, ws.bucket + (now - ws.bt) * 0.15); ws.bt = now;   // ~150 msgs/s per player, bursts of 200
    if (--ws.bucket < 0) return;
    const msg = data.toString();
    if (msg === 'ping') { ws.send('pong'); return; }
    if (msg[0] === 'm') { for (const o of set) if (o !== ws && o.readyState === 1) o.send(msg); }
    else if (msg[0] === 'p' && msg.length < 2000) { try { ws.meta = JSON.parse(msg.slice(1)); presence(set); } catch (e) {} }
  });
  const gone = () => { if (set.delete(ws)) { presence(set); if (!set.size) rooms.delete(room); } };
  ws.on('close', gone); ws.on('error', gone);
});

// drop dead connections (phones that lost signal)
setInterval(() => { for (const set of rooms.values()) for (const w of set) { if (w.lastSeen && Date.now() - w.lastSeen > 45000) { try { w.terminate(); } catch (e) {} } } }, 15000);
wss.on('connection', ws => { ws.lastSeen = Date.now(); ws.on('message', () => { ws.lastSeen = Date.now(); }); });

server.listen(process.env.PORT || 8080, () => console.log('gimmick-server listening'));
