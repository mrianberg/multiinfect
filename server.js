const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { randomBytes } = require('crypto');

const app = express();
const server = http.createServer(app);
// Restrict to the site's own origin (http and https — the site runs on plain
// HTTP today). Set ALLOW_ANY_ORIGIN=1 for local testing.
const ALLOWED_ORIGINS = ['http://ian-berg.com', 'https://ian-berg.com', 'http://www.ian-berg.com', 'https://www.ian-berg.com'];
const io = new Server(server, {
    cors: { origin: process.env.ALLOW_ANY_ORIGIN ? '*' : ALLOWED_ORIGINS },
    maxHttpBufferSize: 1e6, // 1 MB cap on any single packet
});

app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
const MAX_ROOMS = 200;          // global cap
const MAX_MATCH_ENTITIES = 41;  // players + bots

// A crash in one handler must not take down every game. Log and keep serving.
process.on('uncaughtException', (err) => {
    console.error('uncaughtException:', err && err.stack ? err.stack : err);
});

// rooms: Map<roomId, RoomData>
const rooms = new Map();

function generateRoomId() {
    return randomBytes(3).toString('hex').toUpperCase();
}

// Reused for any player-supplied string that gets displayed
function cleanStr(s, max) {
    return String(s == null ? '' : s).slice(0, max).replace(/[<>]/g, '');
}

function getRoomList() {
    const list = [];
    for (const [id, room] of rooms) {
        if (room.state === 'lobby' && room.players.size > 0) {
            list.push({
                id,
                name: room.name,
                hostName: room.hostName,
                playerCount: room.players.size,
            });
        }
    }
    return list;
}

function broadcastRoomList() {
    io.emit('room_list', getRoomList());
}

// In-progress games available to spectate.
function getActiveRoomList() {
    const list = [];
    for (const [id, room] of rooms) {
        if (room.state === 'ingame' && room.players.size > 0) {
            let survivors = 0;
            for (const p of room.players.values()) if (!p.isInfected) survivors++;
            list.push({ id, name: room.name, playerCount: room.players.size, survivors });
        }
    }
    return list;
}

function makePlayer(socket, isHost) {
    return {
        id: socket.id,
        name: socket.playerName,
        ready: false,
        isHost,
        x: 0, y: 1500, z: 0,
        rotY: 0,
        isInfected: false,
        isGliding: true,
        landed: false,
    };
}

io.on('connection', (socket) => {
    socket.playerName = 'Player';
    socket.roomId = null;

    function leaveCurrentRoom() {
        const roomId = socket.roomId;
        if (!roomId) return;
        const room = rooms.get(roomId);
        socket.roomId = null;
        if (!room) return;
        socket.leave(roomId);
        room.players.delete(socket.id);
        io.to(roomId).emit('player_left', { id: socket.id });

        if (room.players.size === 0) {
            // Keep empty rooms briefly — players page-navigate between the
            // lobby and game screens and reconnect; the sweep below removes
            // rooms that stay empty.
            room.emptySince = Date.now();
        } else if (room.hostId === socket.id) {
            // Pass host to the next remaining member (never to a random joiner)
            const nextPlayer = room.players.values().next().value;
            if (nextPlayer) {
                room.hostId = nextPlayer.id;
                nextPlayer.isHost = true;
                io.to(roomId).emit('new_host', { id: nextPlayer.id });
            }
        }
        broadcastRoomList();
    }

    socket.on('set_name', (name) => {
        socket.playerName = cleanStr(name, 20) || 'Player';
    });

    socket.on('get_rooms', () => {
        socket.emit('room_list', getRoomList());
    });

    socket.on('get_active_rooms', () => {
        socket.emit('active_room_list', getActiveRoomList());
    });

    // Spectate an in-progress game: join the socket.io room to receive its
    // broadcasts, but do NOT become a player (not counted, can't act).
    socket.on('spectate_room', (roomId, callback) => {
        if (typeof callback !== 'function') return;
        const room = rooms.get(roomId);
        if (!room || room.state !== 'ingame') return callback({ ok: false, error: 'Game not available' });
        leaveCurrentRoom();
        socket.join(roomId);
        socket.spectatingRoom = roomId;
        callback({
            ok: true, roomId, seed: room.seed, name: room.name,
            hostId: room.hostId, matchPlayers: room.matchPlayerCount || room.players.size,
            players: Array.from(room.players.values()),
            snapshot: {
                botsInfected: Array.from(room.botsInfected),
                infectedPlayers: Array.from(room.infectedPlayers),
                matchElapsed: room.activeAt ? Math.floor((Date.now() - room.activeAt) / 1000) : 0,
            },
        });
    });

    socket.on('create_room', (roomName, callback) => {
        if (typeof callback !== 'function') return;
        if (rooms.size >= MAX_ROOMS) return callback({ ok: false, error: 'Server is full, try again later' });
        leaveCurrentRoom(); // never hold membership in two rooms at once

        const id = generateRoomId();
        const room = {
            id,
            name: cleanStr(roomName, 30) || `${socket.playerName}'s Game`,
            hostId: socket.id,
            hostName: socket.playerName,
            players: new Map(),
            state: 'lobby', // 'lobby' | 'ingame'
            seed: (Math.random() * 4294967296) >>> 0, // shared world-generation seed
            emptySince: null,
            botsInfected: new Set(),
            infectedPlayers: new Set(),
            matchPlayerCount: 1,
            activeAt: 0, // epoch (ms) when the infection/timer phase started
        };
        room.players.set(socket.id, makePlayer(socket, true));
        rooms.set(id, room);
        socket.join(id);
        socket.roomId = id;
        broadcastRoomList();
        callback({ ok: true, roomId: id });
    });

    socket.on('join_room', (roomId, callback) => {
        if (typeof callback !== 'function') return;
        const room = rooms.get(roomId);
        if (!room) return callback({ ok: false, error: 'Room not found' });
        leaveCurrentRoom(); // drop any prior room membership first
        room.emptySince = null;

        // If the previous host never made it into the game (or the room was
        // briefly empty during the lobby→game transition), this joiner hosts.
        if (room.players.size === 0 || !room.players.has(room.hostId)) {
            room.hostId = socket.id;
        }

        const isHost = room.hostId === socket.id;
        room.players.set(socket.id, makePlayer(socket, isHost));
        socket.join(roomId);
        socket.roomId = roomId;

        // Snapshot of live match state so a mid-match (re)joiner is in sync
        const snapshot = room.state === 'ingame' ? {
            botsInfected: Array.from(room.botsInfected),
            infectedPlayers: Array.from(room.infectedPlayers),
            matchElapsed: room.activeAt ? Math.floor((Date.now() - room.activeAt) / 1000) : 0,
        } : null;

        callback({
            ok: true, roomId, players: Array.from(room.players.values()),
            hostId: room.hostId, seed: room.seed, name: room.name,
            state: room.state,
            matchPlayers: room.matchPlayerCount || room.players.size,
            snapshot,
        });

        socket.to(roomId).emit('player_joined', {
            id: socket.id, name: socket.playerName, isHost: false,
        });
        broadcastRoomList();
    });

    socket.on('player_ready', (ready) => {
        const room = rooms.get(socket.roomId);
        if (!room) return;
        const p = room.players.get(socket.id);
        if (p) p.ready = !!ready;
        io.to(socket.roomId).emit('ready_update', { id: socket.id, ready: !!ready });
    });

    socket.on('start_game', () => {
        const room = rooms.get(socket.roomId);
        if (!room || room.hostId !== socket.id) return;
        if (room.state !== 'lobby') return; // ignore double-clicks / stale starts
        room.state = 'ingame';
        // Fresh state for this match
        room.seed = (Math.random() * 4294967296) >>> 0;
        room.botsInfected = new Set();
        room.infectedPlayers = new Set();
        room.matchPlayerCount = room.players.size;
        room.activeAt = 0;
        broadcastRoomList();
        io.to(socket.roomId).emit('game_start');
    });

    // Host marks the active phase start; sets the authoritative match clock.
    socket.on('begin_active', () => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame' || room.hostId !== socket.id) return;
        room.activeAt = Date.now();
        io.to(socket.roomId).emit('begin_active', { activeAt: room.activeAt });
    });

    // Host seeds an infection via lightning (once per minute). Host-authoritative
    // target; relayed to everyone so the strike + infection appear on all clients.
    socket.on('lightning', (data) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame' || room.hostId !== socket.id) return;
        if (!data || typeof data !== 'object') return;
        const out = {};
        if (Number.isInteger(data.bot) && data.bot >= 0 && data.bot < MAX_MATCH_ENTITIES) {
            if (!room.botsInfected.has(data.bot)) { room.botsInfected.add(data.bot); out.bot = data.bot; }
        }
        if (typeof data.player === 'string' && room.players.has(data.player)) {
            const p = room.players.get(data.player);
            if (p && !p.isInfected) { p.isInfected = true; room.infectedPlayers.add(data.player); out.player = data.player; }
        }
        if (out.bot !== undefined || out.player !== undefined) io.to(socket.roomId).emit('lightning', out);
    });

    // Per-frame position update — thin relay, clients handle physics locally
    socket.on('pos_update', (data) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame') return;
        if (!data || typeof data !== 'object') return;
        const p = room.players.get(socket.id);
        if (!p) return;
        p.x = +data.x || 0; p.y = +data.y || 0; p.z = +data.z || 0;
        p.rotY = +data.rotY || 0;
        p.isGliding = !!data.isGliding;
        p.landed = !!data.landed;
        p.mounted = !!data.mounted;
        socket.to(socket.roomId).volatile.emit('pos_update', {
            id: socket.id, x: p.x, y: p.y, z: p.z,
            rotY: p.rotY, isGliding: p.isGliding, landed: p.landed, mounted: p.mounted,
        });
    });

    // Each player broadcasts the slice of bots they simulate
    socket.on('bots_state', (data) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame' || !room.players.has(socket.id)) return;
        if (!Array.isArray(data) || data.length > MAX_MATCH_ENTITIES) return;
        socket.to(socket.roomId).volatile.emit('bots_state', data);
    });

    // Any client can report tagging a bot; dedupe so the count drops once
    socket.on('bot_infected', (data) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame') return;
        const index = data && data.index;
        if (!Number.isInteger(index) || index < 0 || index >= MAX_MATCH_ENTITIES) return;
        if (room.botsInfected.has(index)) return;
        room.botsInfected.add(index);
        socket.to(socket.roomId).emit('bot_infected', { index, byName: cleanStr(data.byName, 20) });
    });

    // Player X infected player Y by contact (Y's client is authoritative for
    // being caught, so we accept X's report only if X is actually infected)
    socket.on('infect', (targetId) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame') return;
        const source = room.players.get(socket.id);
        const target = room.players.get(targetId);
        if (!source || !target || !source.isInfected || target.isInfected) return;
        target.isInfected = true;
        room.infectedPlayers.add(targetId);
        io.to(socket.roomId).emit('infected', { targetId, byId: socket.id, byName: source.name });
    });

    socket.on('i_infected', (data) => {
        // Player reports they became infected (by a bot or storm, no target socket)
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame') return;
        const p = room.players.get(socket.id);
        if (!p || p.isInfected) return;
        p.isInfected = true;
        room.infectedPlayers.add(socket.id);
        const byName = cleanStr(data && data.byName, 20) || '?';
        socket.to(socket.roomId).emit('infected', { targetId: socket.id, byId: null, byName });
    });

    socket.on('game_over', (result) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.hostId !== socket.id) return;
        if (room.state !== 'ingame') return;
        // Return the room to the lobby so the group can play again
        room.state = 'lobby';
        room.activeAt = 0;
        for (const p of room.players.values()) {
            p.ready = false;
            p.isInfected = false;
        }
        room.botsInfected = new Set();
        room.infectedPlayers = new Set();
        const payload = (result && typeof result === 'object')
            ? { result: result.result === 'infection' ? 'infection' : 'survivors' }
            : { result: 'survivors' };
        io.to(socket.roomId).emit('game_over', payload);
        broadcastRoomList();
    });

    socket.on('leave_room', leaveCurrentRoom);
    socket.on('disconnect', leaveCurrentRoom);
});

// Sweep rooms that stay empty for over 2 minutes
setInterval(() => {
    const now = Date.now();
    for (const [id, room] of rooms) {
        if (room.players.size === 0 && room.emptySince && now - room.emptySince > 120000) {
            rooms.delete(id);
        }
    }
}, 60000);

server.listen(PORT, () => {
    console.log(`Infection Tag server running on port ${PORT}`);
});
