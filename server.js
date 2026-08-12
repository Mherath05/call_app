/**
 * WebRTC Socket.io Signaling Server
 * Optimized for Render / Railway Free Tier deployment
 * Handles 1-on-1 Client to Admin call routing & presence status.
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());

// Health check endpoint for Render free tier keep-alive
app.get('/', (req, res) => {
  res.send({ status: 'ok', service: 'WebRTC Signaling Server', online: true });
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  pingTimeout: 60000,
  pingInterval: 25000
});

// In-memory user state tracking
// userId -> { socketId, role }
const activeUsers = new Map();
let adminSocketId = null;
let adminUserId = null;

function isAdminOnline() {
  return adminSocketId !== null && io.sockets.sockets.has(adminSocketId);
}

function broadcastAdminStatus() {
  const isOnline = isAdminOnline();
  io.emit('admin-status-changed', {
    isOnline: isOnline,
    adminId: adminUserId
  });
  console.log(`[Presence] Broadcasted Admin Status: ${isOnline ? 'ONLINE' : 'OFFLINE'}`);
}

io.on('connection', (socket) => {
  console.log(`[Socket Connected] Socket ID: ${socket.id}`);

  // 1. User Registration & Role Assignment
  socket.on('register', (data) => {
    const { userId, role } = data;
    if (!userId || !role) return;

    socket.userId = userId;
    socket.role = role;

    activeUsers.set(userId, { socketId: socket.id, role: role });

    if (role === 'admin') {
      adminSocketId = socket.id;
      adminUserId = userId;
      console.log(`[Admin Registered] Admin ID: ${userId} on Socket: ${socket.id}`);
      broadcastAdminStatus();
    } else {
      console.log(`[Client Registered] Client ID: ${userId} on Socket: ${socket.id}`);
      // Notify client of current Admin status immediately
      socket.emit('admin-status-changed', {
        isOnline: isAdminOnline(),
        adminId: adminUserId
      });
    }
  });

  // 2. Client queries Admin presence status
  socket.on('get-admin-status', (ackCallback) => {
    const isOnline = isAdminOnline();
    const response = { isOnline, adminId: adminUserId };
    if (typeof ackCallback === 'function') {
      ackCallback(response);
    } else {
      socket.emit('admin-status-response', response);
    }
  });

  // 3. WebRTC Call Offer (Client -> Admin)
  socket.on('offer-call', (data) => {
    const { callerId, callerName, offerSdp } = data;
    console.log(`[Call Offer] From Client ${callerName || callerId} (${callerId}) to Admin ${adminUserId}`);

    if (!isAdminOnline()) {
      socket.emit('call-rejected', {
        reason: 'Admin is currently offline.'
      });
      return;
    }

    // Forward incoming call offer to the Admin socket
    io.to(adminSocketId).emit('incoming-call', {
      callerId: callerId,
      callerName: callerName || 'Client',
      callerSocketId: socket.id,
      offerSdp: offerSdp
    });
  });

  // 4. WebRTC Call Answer (Admin -> Client)
  socket.on('answer-call', (data) => {
    const { callerId, answerSdp } = data;
    console.log(`[Call Answered] Admin answered call from Client ${callerId}`);

    const clientUser = activeUsers.get(callerId);
    if (clientUser && clientUser.socketId) {
      io.to(clientUser.socketId).emit('call-accepted', {
        adminId: adminUserId,
        answerSdp: answerSdp
      });
    } else {
      console.log(`[Call Answer Error] Client socket for ${callerId} not found`);
    }
  });

  // 5. ICE Candidate Exchange (Bidirectional)
  socket.on('ice-candidate', (data) => {
    const { targetId, candidate } = data;
    const targetUser = activeUsers.get(targetId);

    if (targetUser && targetUser.socketId) {
      io.to(targetUser.socketId).emit('ice-candidate', {
        senderId: socket.userId,
        candidate: candidate
      });
    }
  });

  // 6. Call Rejection (Admin -> Client)
  socket.on('reject-call', (data) => {
    const { callerId, reason } = data;
    console.log(`[Call Rejected] Admin rejected call from ${callerId}`);

    const clientUser = activeUsers.get(callerId);
    if (clientUser && clientUser.socketId) {
      io.to(clientUser.socketId).emit('call-rejected', {
        reason: reason || 'Admin declined the call.'
      });
    }
  });

  // 7. End Call (Either party)
  socket.on('end-call', (data) => {
    const { targetId } = data;
    console.log(`[Call Ended] Socket ${socket.id} ended call with ${targetId}`);

    const targetUser = activeUsers.get(targetId);
    if (targetUser && targetUser.socketId) {
      io.to(targetUser.socketId).emit('call-ended', {
        by: socket.userId
      });
    }
  });

  // 8. Disconnect Cleanup
  socket.on('disconnect', () => {
    console.log(`[Socket Disconnected] ID: ${socket.id}`);

    if (socket.userId) {
      activeUsers.delete(socket.userId);
    }

    if (socket.id === adminSocketId) {
      adminSocketId = null;
      adminUserId = null;
      console.log(`[Admin Disconnected] Admin is now OFFLINE`);
      broadcastAdminStatus();
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`===================================================`);
  console.log(`🚀 WebRTC Signaling Server listening on port ${PORT}`);
  console.log(`===================================================`);
});
