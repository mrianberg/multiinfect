const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { randomBytes } = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static('public'));

const PORT = process.env.PORT || 3000;

// rooms: Map<roomId, RoomData>
const rooms = new Map();

function generateRoomId() {
    return randomBytes(3).toString('hex').toUpperCase();
}

function getRoomList() {
    const list = [];
    for (const [id, room] of rooms) {
        if (room.state === 'lobby') {
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

io.on('connection', (socket) => {
    socket.playerName = 'Player';
    socket.roomId = null;

    socket.on('set_name', (name) => {
        socket.playerName = String(name).slice(0, 20).replace(/[<>]/g, '') || 'Player';
    });

    socket.on('get_rooms', () => {
        socket.emit('room_list', getRoomList());
    });

    socket.on('create_room', (roomName, callback) => {
        const id = generateRoomId();
        const room = {
            id,
            name: String(roomName).slice(0, 30).replace(/[<>]/g, '') || `${socket.playerName}'s Game`,
            hostId: socket.id,
            hostName: socket.playerName,
            players: new Map(),
            state: 'lobby', // 'lobby' | 'ingame' | 'ended'
            seed: (Math.random() * 4294967296) >>> 0, // shared world-generation seed
            emptySince: null,
        };
        room.players.set(socket.id, {
            id: socket.id,
            name: socket.playerName,
            ready: false,
            isHost: true,
            // game state filled in later
            x: 0, y: 1500, z: 0,
            rotY: 0,
            isInfected: false,
            isGliding: true,
            landed: false,
        });
        rooms.set(id, room);
        socket.join(id);
        socket.roomId = id;
        broadcastRoomList();
        callback({ ok: true, roomId: id });
    });

    socket.on('join_room', (roomId, callback) => {
        const room = rooms.get(roomId);
        if (!room) return callback({ ok: false, error: 'Room not found' });
        // Players rejoin 'ingame' rooms when navigating from lobby page to game page
        if (room.state === 'ended') return callback({ ok: false, error: 'Game has ended' });
        room.emptySince = null;

        // If the previous host never made it into the game (or the room was
        // briefly empty during the lobby→game transition), this joiner hosts.
        // The host client is authoritative for bot simulation.
        if (room.players.size === 0 || !room.players.has(room.hostId)) {
            room.hostId = socket.id;
        }

        room.players.set(socket.id, {
            id: socket.id,
            name: socket.playerName,
            ready: false,
            isHost: false,
            x: 0, y: 1500, z: 0,
            rotY: 0,
            isInfected: false,
            isGliding: true,
            landed: false,
        });
        socket.join(roomId);
        socket.roomId = roomId;

        // Tell the joining player about existing players
        const playerList = Array.from(room.players.values());
        callback({ ok: true, roomId, players: playerList, hostId: room.hostId, seed: room.seed });

        // Tell everyone else in the room
        socket.to(roomId).emit('player_joined', {
            id: socket.id,
            name: socket.playerName,
            isHost: false,
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
        room.state = 'ingame';
        broadcastRoomList();
        io.to(socket.roomId).emit('game_start');
    });

    // Per-frame position update — thin relay, clients handle physics locally
    socket.on('pos_update', (data) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame') return;
        const p = room.players.get(socket.id);
        if (!p) return;
        p.x = data.x; p.y = data.y; p.z = data.z;
        p.rotY = data.rotY;
        p.isGliding = data.isGliding;
        p.landed = data.landed;
        p.mounted = data.mounted;
        // volatile: drop stale packets rather than queueing them (reduces perceived lag)
        socket.to(socket.roomId).volatile.emit('pos_update', {
            id: socket.id, x: data.x, y: data.y, z: data.z,
            rotY: data.rotY, isGliding: data.isGliding, landed: data.landed,
            mounted: data.mounted,
        });
    });

    // Host broadcasts bot positions; everyone else replicates them
    socket.on('bots_state', (data) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame' || room.hostId !== socket.id) return;
        socket.to(socket.roomId).volatile.emit('bots_state', data);
    });

    // Any client can report tagging a bot; dedupe so the count drops once
    socket.on('bot_infected', ({ index, byName }) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame') return;
        if (typeof index !== 'number') return;
        if (!room.botsInfected) room.botsInfected = new Set();
        if (room.botsInfected.has(index)) return;
        room.botsInfected.add(index);
        socket.to(socket.roomId).emit('bot_infected', { index, byName });
    });

    // Authoritative infection events — broadcast to room
    socket.on('infect', (targetId) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.state !== 'ingame') return;
        const source = room.players.get(socket.id);
        const target = room.players.get(targetId);
        if (!source || !target || !source.isInfected || target.isInfected) return;
        target.isInfected = true;
        io.to(socket.roomId).emit('infected', { targetId, byId: socket.id, byName: source.name });
    });

    socket.on('i_infected', (data) => {
        // Player reports they became infected (by a bot or storm, no target socket)
        const room = rooms.get(socket.roomId);
        if (!room) return;
        const p = room.players.get(socket.id);
        if (p) p.isInfected = true;
        socket.to(socket.roomId).emit('infected', { targetId: socket.id, byId: null, byName: data.byName || '?' });
    });

    socket.on('game_over', (result) => {
        const room = rooms.get(socket.roomId);
        if (!room || room.hostId !== socket.id) return;
        room.state = 'ended';
        io.to(socket.roomId).emit('game_over', result);
    });

    socket.on('disconnect', () => {
        const roomId = socket.roomId;
        if (!roomId) return;
        const room = rooms.get(roomId);
        if (!room) return;
        room.players.delete(socket.id);
        io.to(roomId).emit('player_left', { id: socket.id });

        if (room.players.size === 0) {
            // Keep empty ingame rooms briefly — players reconnect while navigating
            // from the lobby page to the game page.
            if (room.state !== 'ingame') rooms.delete(roomId);
            else room.emptySince = Date.now();
        } else if (room.hostId === socket.id) {
            // Pass host to next player
            const nextPlayer = room.players.values().next().value;
            if (nextPlayer) {
                room.hostId = nextPlayer.id;
                nextPlayer.isHost = true;
                io.to(roomId).emit('new_host', { id: nextPlayer.id });
            }
        }
        broadcastRoomList();
    });
});

// Sweep abandoned ingame rooms (empty for over 2 minutes)
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
