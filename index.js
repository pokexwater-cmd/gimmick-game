// Gimmick relay: one Durable Object per room. It stores nothing, it only passes messages along.
// Wire format (client <-> server), plain text for speed:
//   "p{json}"  presence meta of a player (name, color, gimmick...)   client -> server
//   "m{json}"  a game message                                        client -> server, relayed to everybody else as-is
//   {"k":"presence","state":{id:meta}}                               server -> client (who is in the room)
//   "ping" / "pong"                                                  answered by Cloudflare itself (free, keeps the socket alive)
import { DurableObject } from 'cloudflare:workers';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('gimmick-server ok', { headers: { 'access-control-allow-origin': '*' } });
    const room = (url.searchParams.get('room') || 'lobby').slice(0, 64);
    return env.ROOM.get(env.ROOM.idFromName(room)).fetch(request);
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }
  async fetch(request) {
    const id = (new URL(request.url).searchParams.get('id') || '').slice(0, 16);
    if (!id) return new Response('missing id', { status: 400 });
    for (const old of this.ctx.getWebSockets(id)) { try { old.close(1000, 'replaced'); } catch (e) {} }   // same player reconnecting
    const pair = new WebSocketPair(), [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server, [id]);
    server.serializeAttachment({ id, meta: null });
    this.sendTo(server);
    return new Response(null, { status: 101, webSocket: client });
  }
  state() {
    const o = {};
    for (const ws of this.ctx.getWebSockets()) { const a = ws.deserializeAttachment(); if (a && a.meta) o[a.id] = a.meta; }
    return o;
  }
  sendTo(ws) { try { ws.send(JSON.stringify({ k: 'presence', state: this.state() })); } catch (e) {} }
  presence() { const s = JSON.stringify({ k: 'presence', state: this.state() }); for (const ws of this.ctx.getWebSockets()) { try { ws.send(s); } catch (e) {} } }
  async webSocketMessage(ws, msg) {
    if (typeof msg !== 'string' || msg.length > 60000) return;
    const t = msg[0];
    if (t === 'm') { for (const o of this.ctx.getWebSockets()) if (o !== ws) { try { o.send(msg); } catch (e) {} } }   // relay untouched, no parsing
    else if (t === 'p') {
      try { const a = ws.deserializeAttachment(); a.meta = JSON.parse(msg.slice(1)); ws.serializeAttachment(a); this.presence(); } catch (e) {}
    }
  }
  async webSocketClose(ws, code) { try { ws.close(code, 'bye'); } catch (e) {} this.presence(); }
  async webSocketError(ws) { this.presence(); }
}
