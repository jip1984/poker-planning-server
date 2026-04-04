import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';

const app = express();
const CLIENT_ORIGINS = (process.env.CLIENT_ORIGIN ?? 'http://localhost:3000')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

app.use(cors({
  origin: CLIENT_ORIGINS,
  credentials: true,
}));
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: CLIENT_ORIGINS,
    credentials: true,
  },
});
const ROOM_ID_LENGTH = 6;
const PORT = Number(process.env.PORT ?? 4000);

type Role = 'host' | 'voter';
type Vote = number | '?' | null;

interface User {
  id: string;
  name: string;
  role: Role;
  vote: Vote;
}

interface RoomState {
  ticket: string;
  revealed: boolean;
  users: User[];
}

const rooms: Record<string, RoomState> = {};

io.on('connection', (socket) => {
  socket.on('create_room', (callback: (roomId: string) => void) => {
    const roomId = generateRoomId();
    rooms[roomId] = { ticket: '', revealed: false, users: [] };
    callback(roomId);
  });

  socket.on('join_room', ({ roomId, userName, role }) => {
    socket.join(roomId);
    if (!rooms[roomId]) rooms[roomId] = { ticket: '', revealed: false, users: [] };
    rooms[roomId].users = rooms[roomId].users.filter((user) => user.id !== socket.id);
    rooms[roomId].users.push({ id: socket.id, name: userName, role, vote: null });
    io.to(roomId).emit('update_state', rooms[roomId]);
  });

  socket.on('update_ticket', ({ roomId, ticket }) => {
    if (rooms[roomId]) {
      rooms[roomId].ticket = ticket;
      io.to(roomId).emit('update_state', rooms[roomId]);
    }
  });

  socket.on('cast_vote', ({ roomId, vote }) => {
    const user = rooms[roomId]?.users.find((u) => u.id === socket.id);
    if (user) { user.vote = vote; io.to(roomId).emit('update_state', rooms[roomId]); }
  });

  socket.on('reveal', (roomId) => {
    if (rooms[roomId]) { rooms[roomId].revealed = true; io.to(roomId).emit('update_state', rooms[roomId]); }
  });

  // NEW: Next Ticket (Clears title and votes)
  socket.on('next_round', (roomId) => {
    if (rooms[roomId]) {
      rooms[roomId].ticket = '';
      rooms[roomId].revealed = false;
      rooms[roomId].users.forEach((u) => u.vote = null);
      io.to(roomId).emit('update_state', rooms[roomId]);
    }
  });

  socket.on('leave_room', (roomId) => {
    removeUserFromRoom(roomId, socket.id);
    socket.leave(roomId);
  });

  socket.on('disconnect', () => {
    for (const rid in rooms) {
      removeUserFromRoom(rid, socket.id);
    }
  });
});

httpServer.listen(PORT, () => console.log(`Server: http://localhost:${PORT}`));

function generateRoomId() {
  let roomId = '';

  do {
    roomId = Math.random().toString(36).slice(2, 2 + ROOM_ID_LENGTH);
  } while (rooms[roomId]);

  return roomId;
}

function removeUserFromRoom(roomId: string, socketId: string) {
  const room = rooms[roomId];
  if (!room) return;

  room.users = room.users.filter((u) => u.id !== socketId);

  if (room.users.length === 0) {
    delete rooms[roomId];
    return;
  }

  io.to(roomId).emit('update_state', room);
}
