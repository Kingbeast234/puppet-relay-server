// PUPPET relay server
//
// This is a tiny "dumb" relay: it never looks at what's inside a message,
// it just matches two players into a room and forwards whatever one of
// them sends straight to the other. All the game logic still lives in the
// browser (puppet.html) — this server only exists so two players on two
// different networks can find each other and exchange messages reliably.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"; // no 0/O/1/I/L — avoids confusion
const ROOM_TTL_MS = 1000 * 60 * 60 * 6; // clean up an abandoned room after 6 hours

// code -> { seats: [wsOrNull, wsOrNull], createdAt: number }
const rooms = new Map();

function genCode() {
  let code;
  do {
    code = "";
    for (let i = 0; i < 5; i++) code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
  } while (rooms.has(code));
  return code;
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (err) { /* ignore */ }
  }
}

function cleanupRoom(code) {
  const room = rooms.get(code);
  if (room && !room.seats[0] && !room.seats[1]) rooms.delete(code);
}

const server = http.createServer((req, res) => {
  // Simple health check page — also what you'll see if you visit the
  // server's URL directly in a browser.
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("PUPPET relay server is running. Rooms open: " + rooms.size + "\n");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  ws.roomCode = null;
  ws.seat = null;
  ws.isAlive = true;

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (err) { return; }
    if (!msg || typeof msg.type !== "string") return;

    if (msg.type === "create") {
      const code = genCode();
      rooms.set(code, { seats: [ws, null], createdAt: Date.now() });
      ws.roomCode = code;
      ws.seat = 1;
      send(ws, { type: "created", code });
      return;
    }

    if (msg.type === "join") {
      const code = String(msg.code || "").toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) { send(ws, { type: "error", reason: "not-found" }); return; }
      if (room.seats[1]) { send(ws, { type: "error", reason: "full" }); return; }
      room.seats[1] = ws;
      ws.roomCode = code;
      ws.seat = 2;
      send(ws, { type: "joined", code });
      send(room.seats[0], { type: "peer-joined" });
      return;
    }

    // Anything else (line/chat/sync/meta/...): forward untouched to
    // whichever seat isn't the sender.
    if (!ws.roomCode) return;
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    const otherSeatIdx = ws.seat === 1 ? 1 : 0;
    send(room.seats[otherSeatIdx], msg);
  });

  ws.on("close", () => {
    if (!ws.roomCode) return;
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    const otherSeatIdx = ws.seat === 1 ? 1 : 0;
    send(room.seats[otherSeatIdx], { type: "peer-left" });
    room.seats[ws.seat - 1] = null;
    cleanupRoom(ws.roomCode);
  });
});

// Heartbeat: drop connections that stopped responding (e.g. a laptop that
// went to sleep) so their room seat frees up instead of staying stuck.
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) { ws.terminate(); return; }
    ws.isAlive = false;
    try { ws.ping(); } catch (err) { /* ignore */ }
  });
}, 30000);

// Sweep abandoned rooms every hour, just in case a room never gets a close event.
const sweep = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.createdAt > ROOM_TTL_MS) rooms.delete(code);
  }
}, 1000 * 60 * 60);

wss.on("close", () => { clearInterval(heartbeat); clearInterval(sweep); });

server.listen(PORT, () => {
  console.log("PUPPET relay server listening on port " + PORT);
});
