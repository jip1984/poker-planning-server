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
// How long a dropped user keeps their seat (and vote) before being removed.
const DISCONNECT_GRACE_MS = 30_000;

const io = new Server(httpServer, {
  cors: {
    origin: CLIENT_ORIGINS,
    credentials: true,
  },
  // Lets a briefly dropped socket come back with the same id and rooms,
  // and replays any events it missed.
  connectionStateRecovery: {
    maxDisconnectionDuration: DISCONNECT_GRACE_MS,
  },
});
const ROOM_ID_LENGTH = 6;
const PORT = Number(process.env.PORT ?? 4000);
const MIN_NAME_LENGTH = 3;
const MAX_NAME_LENGTH = 40;
const MAX_JOB_ROLE_LENGTH = 30;
const MAX_TICKET_LENGTH = 200;
const MAX_CARDS = 12;
const MAX_CARD_VALUE = 999;
const MAX_CARD_SET_LABEL_LENGTH = 40;

// Server-only: who created each room. Only that browser tab may join as host.
const roomCreators = new Map<string, { socketId: string; clientId: string | null }>();

// Server-only: socket id -> the browser tab's stable client id. Kept out of
// RoomState so it's never broadcast to other users.
const clientIdBySocket = new Map<string, string>();
// Socket id -> timer that removes the user once the grace period ends.
const pendingRemovals = new Map<string, ReturnType<typeof setTimeout>>();

type Role = 'host' | 'voter';
type CardValue = number | string;

interface CardSet {
  name: string;
  label: string;
  cards: CardValue[];
  scoreValues: number[];
}

interface User {
  id: string;
  name: string;
  role: Role;
  jobRole: string;
  vote: CardValue | null;
}

interface TicketHistoryEntry {
  ticket: string;
  score: CardValue;
  completedAt: string;
}

interface RoomState {
  ticket: string;
  revealed: boolean;
  users: User[];
  history: TicketHistoryEntry[];
  cardSet: CardSet;
  autoReveal: boolean;
}

const DEFAULT_CARD_SET: CardSet = {
  name: 'fibonacci',
  label: 'Fibonacci',
  cards: [1, 3, 5, 8, 13, '?'],
  scoreValues: [1, 3, 5, 8, 13],
};

const rooms: Record<string, RoomState> = {};

io.on('connection', (socket) => {
  // The room, but only if this socket is its host.
  const roomIfHost = (roomId: unknown) => {
    if (typeof roomId !== 'string') return null;
    const room = rooms[roomId];
    const user = room?.users.find((u) => u.id === socket.id);
    return room && user?.role === 'host' ? room : null;
  };

  socket.on('create_room', (callback: (roomId: string) => void) => {
    if (typeof callback !== 'function') return;
    const roomId = generateRoomId();
    rooms[roomId] = { ticket: '', revealed: false, users: [], history: [], cardSet: DEFAULT_CARD_SET, autoReveal: false };
    roomCreators.set(roomId, { socketId: socket.id, clientId: null });
    callback(roomId);
  });

  if (socket.recovered) cancelPendingRemoval(socket.id);

  socket.on('join_room', (payload) => {
    const { roomId, userName, role, jobRole, clientId } = payload ?? {};
    if (typeof roomId !== 'string' || typeof userName !== 'string') return;
    const nameError = validateName(userName);
    if (nameError) {
      socket.emit('join_error', nameError);
      return;
    }
    const normalizedUserName = userName.trim().toLowerCase();
    const cleanJobRole = cleanText(jobRole, MAX_JOB_ROLE_LENGTH);

    // Rooms only come from create_room; don't let a join invent one.
    const room = rooms[roomId];
    if (!room) {
      socket.emit('join_error', 'This room no longer exists. Ask the host for a new link.');
      return;
    }

    socket.join(roomId);
    const validClientId = typeof clientId === 'string' && clientId.length > 0 && clientId.length <= 100 ? clientId : null;
    if (validClientId) clientIdBySocket.set(socket.id, validClientId);

    // Same socket, or the same browser tab coming back on a new socket.
    const existingUser = room.users.find((user) => (
      user.id === socket.id || (validClientId !== null && clientIdBySocket.get(user.id) === validClientId)
    ));

    const duplicateNameExists = room.users.some((user) => (
      user !== existingUser && user.name.trim().toLowerCase() === normalizedUserName
    ));

    if (duplicateNameExists) {
      socket.leave(roomId);
      socket.emit('join_error', 'That name is already in use for this room.');
      return;
    }

    if (existingUser) {
      // Rejoin: keep their seat, role and vote, move them onto this socket.
      const oldSocketId = existingUser.id;
      if (oldSocketId !== socket.id) {
        cancelPendingRemoval(oldSocketId);
        clientIdBySocket.delete(oldSocketId);
        io.sockets.sockets.get(oldSocketId)?.leave(roomId);
      }
      existingUser.id = socket.id;
      existingUser.name = userName;
      existingUser.jobRole = cleanJobRole;
      if (isObserver(existingUser)) existingUser.vote = null;
    } else {
      let assignedRole: Role = 'voter';

      if (role === 'host') {
        // Only the tab that created the room may be host, and only one at a time.
        const creator = roomCreators.get(roomId);
        const isCreator = creator !== undefined && (
          creator.socketId === socket.id || (validClientId !== null && creator.clientId === validClientId)
        );
        const hasHost = room.users.some((u) => u.role === 'host');

        if (!isCreator || hasHost) {
          socket.leave(roomId);
          socket.emit('join_error', 'Only the person who created this room can join as host.');
          return;
        }

        creator.socketId = socket.id;
        creator.clientId = validClientId;
        assignedRole = 'host';
      }

      room.users.push({ id: socket.id, name: userName, role: assignedRole, jobRole: cleanJobRole, vote: null });
    }

    io.to(roomId).emit('update_state', room);
  });

  socket.on('update_ticket', (payload) => {
    const { roomId, ticket } = payload ?? {};
    const room = roomIfHost(roomId);
    if (!room || typeof ticket !== 'string') return;

    if (ticket.length > MAX_TICKET_LENGTH) {
      socket.emit('ticket_update_error', `Ticket must be ${MAX_TICKET_LENGTH} characters or fewer.`);
      return;
    }

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

  socket.on('update_profile', (payload) => {
    const { roomId, userName, jobRole } = payload ?? {};
    if (typeof roomId !== 'string' || typeof userName !== 'string') return;
    const room = rooms[roomId];
    const user = room?.users.find((existingUser) => existingUser.id === socket.id);
    if (!room || !user) return;

    const nameError = validateName(userName);
    if (nameError) {
      socket.emit('profile_update_error', nameError);
      return;
    }
    const normalizedUserName = userName.trim().toLowerCase();

    const duplicateNameExists = room.users.some((existingUser) => (
      existingUser.id !== socket.id && existingUser.name.trim().toLowerCase() === normalizedUserName
    ));

    if (duplicateNameExists) {
      socket.emit('profile_update_error', 'That name is already in use for this room.');
      return;
    }

    user.name = userName;
    user.jobRole = cleanText(jobRole, MAX_JOB_ROLE_LENGTH);
    // Observers watch only: switching to Observer drops any vote already cast.
    if (isObserver(user)) user.vote = null;
    socket.emit('profile_update_success');
    // Losing a pending voter can mean everyone left has now voted.
    handleAllVoted(room);
    io.to(roomId).emit('update_state', room);
  });

  socket.on('cast_vote', (payload) => {
    const { roomId, vote } = payload ?? {};
    if (typeof roomId !== 'string') return;
    const room = rooms[roomId];
    const user = room?.users.find((u) => u.id === socket.id);
    if (!room || !user) return;
    if (!room.ticket.trim() || room.revealed) return;
    if (isObserver(user)) return; // observers watch only

    // Store the card's own value so "5" and 5 can't both end up in the votes.
    const card = room.cardSet.cards.find((c) => String(c) === String(vote));
    if (vote !== null && card === undefined) return;

    user.vote = vote === null ? null : card!;
    handleAllVoted(room);
    io.to(roomId).emit('update_state', room);
  });

  socket.on('reveal', (roomId) => {
    const room = roomIfHost(roomId);
    if (!room) return;
    room.revealed = true;
    io.to(roomId).emit('update_state', room);
  });

  socket.on('next_round', (roomId) => {
    const room = roomIfHost(roomId);
    if (!room) return;

    const historyEntry = buildHistoryEntry(room);
    if (historyEntry) room.history.unshift(historyEntry);

    room.ticket = '';
    room.revealed = false;
    room.users.forEach((u) => u.vote = null);
    io.to(roomId).emit('update_state', room);
  });

  socket.on('update_card_set', (payload) => {
    const { roomId, cardSet } = payload ?? {};
    const room = roomIfHost(roomId);
    if (!room) return;
    if (room.revealed || room.ticket.trim()) return;

    const cleanCardSet = sanitizeCardSet(cardSet);
    if (!cleanCardSet) return;

    room.cardSet = cleanCardSet;
    room.users.forEach((u) => u.vote = null);
    io.to(roomId).emit('update_state', room);
  });

  socket.on('toggle_auto_reveal', (roomId) => {
    const room = roomIfHost(roomId);
    if (!room) return;
    room.autoReveal = !room.autoReveal;
    io.to(roomId).emit('update_state', room);
  });

  socket.on('leave_room', (roomId) => {
    if (typeof roomId !== 'string') return;
    cancelPendingRemoval(socket.id);
    removeUserFromRoom(roomId, socket.id);
    socket.leave(roomId);
  });

  socket.on('disconnect', (reason) => {
    // The client closed the socket on purpose: remove them now.
    if (reason === 'client namespace disconnect') {
      removeUserEverywhere(socket.id);
      return;
    }

    // Network drop, sleep, refresh: hold their seat so they can rejoin.
    cancelPendingRemoval(socket.id);
    pendingRemovals.set(socket.id, setTimeout(() => {
      pendingRemovals.delete(socket.id);
      removeUserEverywhere(socket.id);
    }, DISCONNECT_GRACE_MS));
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

function cancelPendingRemoval(socketId: string) {
  const timer = pendingRemovals.get(socketId);
  if (timer) clearTimeout(timer);
  pendingRemovals.delete(socketId);
}

function removeUserEverywhere(socketId: string) {
  for (const rid in rooms) {
    removeUserFromRoom(rid, socketId);
  }
  clientIdBySocket.delete(socketId);
}

function removeUserFromRoom(roomId: string, socketId: string) {
  const room = rooms[roomId];
  if (!room) return;

  room.users = room.users.filter((u) => u.id !== socketId);

  if (room.users.length === 0) {
    delete rooms[roomId];
    roomCreators.delete(roomId);
    return;
  }

  io.to(roomId).emit('update_state', room);
}

function isObserver(user: User) {
  return normalizeJobRole(user.jobRole) === 'observer';
}

/** Once every voter has voted: reveal (auto-reveal on) or tell the host. */
function handleAllVoted(room: RoomState) {
  if (!shouldAutoReveal(room)) return;

  if (room.autoReveal) {
    room.revealed = true;
  } else {
    const host = room.users.find((u) => u.role === 'host');
    if (host) io.to(host.id).emit('all_voted');
  }
}

function shouldAutoReveal(room: RoomState) {
  const requiredVoters = room.users.filter((user) => (
    user.role === 'voter' && !isObserver(user)
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

function calculateRoundScore(room: RoomState): CardValue | null {
  const submittedVotes = room.users
    .map((user) => user.vote)
    .filter((vote): vote is CardValue => vote !== null);

  if (!submittedVotes.length) return null;

  if (submittedVotes.every((vote) => String(vote) === '?')) {
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

  const scoreValues = room.cardSet.scoreValues;
  if (!scoreValues.length) return '?';

  return scoreValues.reduce((closest, current) => {
    return Math.abs(current - median) < Math.abs(closest - median) ? current : closest;
  }, scoreValues[0]!);
}

function validateName(userName: string): string | null {
  const trimmed = userName.trim();
  if (!trimmed) return 'Please enter a name.';
  if (trimmed.length < MIN_NAME_LENGTH) return `Name must be at least ${MIN_NAME_LENGTH} characters.`;
  if (userName.length > MAX_NAME_LENGTH) return `Name must be ${MAX_NAME_LENGTH} characters or fewer.`;
  return null;
}

// Any non-string becomes '', and long strings are cut to maxLength.
function cleanText(value: unknown, maxLength: number) {
  return typeof value === 'string' ? value.trim().slice(0, maxLength) : '';
}

// Rebuilds a card set from client input. Cards must be '?' or a number
// from 0 to MAX_CARD_VALUE; score values are derived here, not trusted.
function sanitizeCardSet(input: unknown): CardSet | null {
  if (!input || typeof input !== 'object') return null;
  const { name, label, cards } = input as Record<string, unknown>;
  if (!Array.isArray(cards) || cards.length < 2 || cards.length > MAX_CARDS) return null;

  const cleanCards: CardValue[] = [];
  for (const card of cards) {
    if (card === '?') {
      cleanCards.push('?');
    } else if (typeof card === 'number' && Number.isFinite(card) && card >= 0 && card <= MAX_CARD_VALUE) {
      cleanCards.push(card);
    } else {
      return null;
    }
  }

  return {
    name: cleanText(name, MAX_CARD_SET_LABEL_LENGTH) || 'custom',
    label: cleanText(label, MAX_CARD_SET_LABEL_LENGTH) || 'Custom',
    cards: cleanCards,
    scoreValues: cleanCards.filter((card): card is number => typeof card === 'number'),
  };
}

function normalizeTicket(ticket: string) {
  return ticket.trim().toLowerCase();
}

function normalizeJobRole(jobRole: string) {
  return jobRole.trim().toLowerCase();
}
