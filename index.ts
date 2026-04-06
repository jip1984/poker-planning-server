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
  jobRole: string;
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

  socket.on('join_room', ({ roomId, userName, role, jobRole = '' }) => {
    const normalizedUserName = userName.trim().toLowerCase();

    if (!normalizedUserName) {
      socket.emit('join_error', 'Please enter a name.');
      return;
    }

    socket.join(roomId);
    if (!rooms[roomId]) rooms[roomId] = { ticket: '', revealed: false, users: [] };
    const duplicateNameExists = rooms[roomId].users.some((user) => (
      user.id !== socket.id && user.name.trim().toLowerCase() === normalizedUserName
    ));

    if (duplicateNameExists) {
      socket.leave(roomId);
      socket.emit('join_error', 'That name is already in use for this room.');
      return;
    }

    rooms[roomId].users = rooms[roomId].users.filter((user) => user.id !== socket.id);
    rooms[roomId].users.push({ id: socket.id, name: userName, role, jobRole, vote: null });
    io.to(roomId).emit('update_state', rooms[roomId]);
  });

  socket.on('update_ticket', ({ roomId, ticket }) => {
    if (rooms[roomId]) {
      rooms[roomId].ticket = ticket;
      io.to(roomId).emit('update_state', rooms[roomId]);
    }
  });

  socket.on('update_profile', ({ roomId, userName, jobRole = '' }) => {
    const room = rooms[roomId];
    const user = room?.users.find((existingUser) => existingUser.id === socket.id);
    const normalizedUserName = userName.trim().toLowerCase();

    if (!room || !user) return;

    if (!normalizedUserName) {
      socket.emit('profile_update_error', 'Please enter a name.');
      return;
    }

    const duplicateNameExists = room.users.some((existingUser) => (
      existingUser.id !== socket.id && existingUser.name.trim().toLowerCase() === normalizedUserName
    ));

    if (duplicateNameExists) {
      socket.emit('profile_update_error', 'That name is already in use for this room.');
      return;
    }

    user.name = userName;
    user.jobRole = jobRole;
    socket.emit('profile_update_success');
    io.to(roomId).emit('update_state', room);
  });

  socket.on('cast_vote', ({ roomId, vote }) => {
    const room = rooms[roomId];
    const user = room?.users.find((u) => u.id === socket.id);
    if (!room || !user) return;
    if (!room.ticket.trim() || room.revealed) return;

    user.vote = vote;

    if (shouldAutoReveal(room)) {
      room.revealed = true;
    }

    io.to(roomId).emit('update_state', room);
  });

  socket.on('reveal', (roomId) => {
    if (rooms[roomId]) {
      rooms[roomId].revealed = true;
      io.to(roomId).emit('update_state', rooms[roomId]);
    }
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

function shouldAutoReveal(room: RoomState) {
  const voters = room.users.filter((user) => user.role === 'voter');

  return voters.length > 0 && !room.revealed && voters.every((user) => user.vote !== null);
}
