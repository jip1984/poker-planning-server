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
const SCORE_VALUES = [1, 3, 5, 8, 13];
const MIN_NAME_LENGTH = 3;

type Role = 'host' | 'voter';
type Vote = number | '?' | null;

interface User {
  id: string;
  name: string;
  role: Role;
  jobRole: string;
  vote: Vote;
}

interface TicketHistoryEntry {
  ticket: string;
  score: number | '?';
  completedAt: string;
}

interface RoomState {
  ticket: string;
  revealed: boolean;
  users: User[];
  history: TicketHistoryEntry[];
}

const rooms: Record<string, RoomState> = {};

io.on('connection', (socket) => {
  socket.on('create_room', (callback: (roomId: string) => void) => {
    const roomId = generateRoomId();
    rooms[roomId] = { ticket: '', revealed: false, users: [], history: [] };
    callback(roomId);
  });

  socket.on('join_room', ({ roomId, userName, role, jobRole = '' }) => {
    const normalizedUserName = userName.trim().toLowerCase();

    if (!normalizedUserName) {
      socket.emit('join_error', 'Please enter a name.');
      return;
    }

    if (normalizedUserName.length < MIN_NAME_LENGTH) {
      socket.emit('join_error', `Name must be at least ${MIN_NAME_LENGTH} characters.`);
      return;
    }

    socket.join(roomId);
    if (!rooms[roomId]) rooms[roomId] = { ticket: '', revealed: false, users: [], history: [] };
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
    const room = rooms[roomId];
    if (!room) return;

    const normalizedTicket = normalizeTicket(ticket);
    const duplicateTicketExists = normalizedTicket.length > 0 && room.history.some((entry) => (
      normalizeTicket(entry.ticket) === normalizedTicket
    ));

    if (duplicateTicketExists) {
      socket.emit('ticket_update_error', 'This ticket has already been estimated in this session.');
      return;
    }

    room.ticket = ticket;
    io.to(roomId).emit('update_state', room);
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

    if (normalizedUserName.length < MIN_NAME_LENGTH) {
      socket.emit('profile_update_error', `Name must be at least ${MIN_NAME_LENGTH} characters.`);
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
      const historyEntry = buildHistoryEntry(rooms[roomId]);

      if (historyEntry) {
        rooms[roomId].history.unshift(historyEntry);
      }

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
  const requiredVoters = room.users.filter((user) => (
    user.role === 'voter' && normalizeJobRole(user.jobRole) !== 'observer'
  ));

  return requiredVoters.length > 0 && !room.revealed && requiredVoters.every((user) => user.vote !== null);
}

function buildHistoryEntry(room: RoomState): TicketHistoryEntry | null {
  const ticket = room.ticket.trim();

  if (!ticket) return null;

  const score = calculateRoundScore(room);

  if (score === null) return null;

  return {
    ticket,
    score,
    completedAt: new Date().toISOString(),
  };
}

function calculateRoundScore(room: RoomState): number | '?' | null {
  const submittedVotes = room.users
    .map((user) => user.vote)
    .filter((vote): vote is number | '?' => vote !== null);

  if (!submittedVotes.length) return null;

  if (submittedVotes.every((vote) => vote === '?')) {
    return '?';
  }

  const numericVotes = room.users
    .filter((user): user is User & { vote: number } => typeof user.vote === 'number')
    .map((user) => user.vote)
    .sort((a, b) => a - b);

  if (!numericVotes.length) return '?';

  const mid = Math.floor(numericVotes.length / 2);
  const median = numericVotes.length % 2 !== 0
    ? numericVotes[mid]!
    : (numericVotes[mid - 1]! + numericVotes[mid]!) / 2;

  return SCORE_VALUES.reduce((closest, current) => {
    return Math.abs(current - median) < Math.abs(closest - median) ? current : closest;
  }, SCORE_VALUES[0]!);
}

function normalizeTicket(ticket: string) {
  return ticket.trim().toLowerCase();
}

function normalizeJobRole(jobRole: string) {
  return jobRole.trim().toLowerCase();
}
