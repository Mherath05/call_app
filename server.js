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

function getActiveClientCount() {
  let count = 0;
  for (const user of activeUsers.values()) {
    if (user.role === 'client' && io.sockets.sockets.has(user.socketId)) {
      count++;
    }
  }
  return count;
}

function broadcastAdminStatus() {
  const isOnline = isAdminOnline();
  const activeClientCount = getActiveClientCount();

  const payload = {
    isOnline: isOnline,
    adminId: adminUserId,
    activeClientCount: activeClientCount,
    timestamp: Date.now()
  };

  io.emit('admin-status-changed', payload);
  io.emit('presence-update', payload);
  console.log(`[Presence] Broadcasted Presence: Admin=${isOnline ? 'ONLINE' : 'OFFLINE'}, Active Clients=${activeClientCount}`);
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
    } else {
      console.log(`[Client Registered] Client ID: ${userId} on Socket: ${socket.id}`);
    }

    broadcastAdminStatus();
  });

  // 2. Client queries Admin presence status
  socket.on('get-admin-status', (ackCallback) => {
    const isOnline = isAdminOnline();
    const activeClientCount = getActiveClientCount();
    const response = { isOnline, adminId: adminUserId, activeClientCount };
    
    if (typeof ackCallback === 'function') {
      ackCallback(response);
    }
    socket.emit('admin-status-response', response);
    socket.emit('admin-status-changed', response);
    socket.emit('presence-update', response);
  });

  // 3. WebRTC Call Offer (Client -> Admin)
  socket.on('offer-call', (data) => {
    const { callerId, callerName, isVideoCall, offerSdp } = data;
    console.log(`[Call Offer] From Client ${callerName || callerId} (${callerId}) to Admin ${adminUserId} [Video: ${!!isVideoCall}]`);

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
      isVideoCall: !!isVideoCall,
      callerSocketId: socket.id,
      offerSdp: offerSdp
    });
    console.log(`[Call Offer] Forwarded to Admin socket ${adminSocketId}`);
  });

  // 4. WebRTC Call Answer (Admin -> Client)
  socket.on('answer-call', (data) => {
    const { callerId, callerSocketId, answerSdp } = data;
    console.log(`[Call Answered] Admin answered call from Client ${callerId} (Socket: ${callerSocketId || 'unknown'})`);

    let targetSocketId = null;
    if (callerSocketId && io.sockets.sockets.has(callerSocketId)) {
      targetSocketId = callerSocketId;
    }

    if (!targetSocketId && callerId) {
      const clientUser = activeUsers.get(callerId);
      if (clientUser && clientUser.socketId && io.sockets.sockets.has(clientUser.socketId)) {
        targetSocketId = clientUser.socketId;
      }
    }

    if (targetSocketId) {
      io.to(targetSocketId).emit('call-accepted', {
        adminId: adminUserId,
        adminSocketId: adminSocketId,
        answerSdp: answerSdp
      });
      console.log(`[Call Answered] Sent call-accepted to socket ${targetSocketId}`);
    } else {
      console.log(`[Call Answer Error] Client socket for ${callerId} not found`);
    }
  });

  // 5. ICE Candidate Exchange (Bidirectional)
  socket.on('ice-candidate', (data) => {
    const { targetId, targetSocketId, candidate } = data;
    let destinationSocketId = null;

    if (targetSocketId && io.sockets.sockets.has(targetSocketId)) {
      destinationSocketId = targetSocketId;
    }

    if (!destinationSocketId && targetId) {
      const targetUser = activeUsers.get(targetId);
      if (targetUser && targetUser.socketId && io.sockets.sockets.has(targetUser.socketId)) {
        destinationSocketId = targetUser.socketId;
      }
    }

    // Fallback if client is sending ICE candidates to Admin
    if (!destinationSocketId) {
      if (socket.role === 'client' || targetId === 'admin_user_id' || targetId === adminUserId) {
        destinationSocketId = adminSocketId;
      }
    }

    if (destinationSocketId && io.sockets.sockets.has(destinationSocketId)) {
      io.to(destinationSocketId).emit('ice-candidate', {
        senderId: socket.userId,
        senderSocketId: socket.id,
        candidate: candidate
      });
      console.log(`[ICE Candidate] Forwarded from ${socket.userId || socket.id} to socket ${destinationSocketId}`);
    } else {
      console.log(`[ICE Candidate Warning] Destination socket for target ${targetId || targetSocketId} not active`);
    }
  });

  // 6. Call Rejection (Admin -> Client)
  socket.on('reject-call', (data) => {
    const { callerId, callerSocketId, reason } = data;
    console.log(`[Call Rejected] Admin rejected call from ${callerId}`);

    const clientUser = activeUsers.get(callerId);
    const targetSocketId = (clientUser && clientUser.socketId) ? clientUser.socketId : callerSocketId;

    if (targetSocketId) {
      io.to(targetSocketId).emit('call-rejected', {
        reason: reason || 'Admin declined the call.'
      });
    }
  });

  // 6b. Real-Time Text Message (Admin -> Client)
  socket.on('send-text-message', (data) => {
    const { targetId, targetSocketId, text } = data;
    console.log(`[Text Message] From ${socket.userId} to ${targetId}: ${text}`);

    let destinationSocketId = targetSocketId;
    if (!destinationSocketId && targetId) {
      const targetUser = activeUsers.get(targetId);
      if (targetUser) {
        destinationSocketId = targetUser.socketId;
      }
    }

    if (destinationSocketId) {
      io.to(destinationSocketId).emit('receive-text-message', {
        senderId: socket.userId,
        text: text
      });
    } else {
      console.log(`[Text Message Error] Destination socket for ${targetId} not found`);
    }
  });

  // 7. End Call (Either party)
  socket.on('end-call', (data) => {
    const { targetId, targetSocketId } = data;
    console.log(`[Call Ended] Socket ${socket.id} (${socket.role}) ended call with target ${targetId}`);

    let destinationSocketId = targetSocketId;
    if (!destinationSocketId && targetId) {
      const targetUser = activeUsers.get(targetId);
      if (targetUser) {
        destinationSocketId = targetUser.socketId;
      }
    }

    // Fallback if client ends call with Admin
    if (!destinationSocketId && (socket.role === 'client' || targetId === 'admin_user_id')) {
      destinationSocketId = adminSocketId;
    }

    if (destinationSocketId) {
      io.to(destinationSocketId).emit('call-ended', {
        by: socket.userId
      });
      console.log(`[Call Ended] Forwarded call-ended to socket ${destinationSocketId}`);
    } else {
      // Fallback broadcast to admin if available
      if (adminSocketId && socket.id !== adminSocketId) {
        io.to(adminSocketId).emit('call-ended', { by: socket.userId });
      }
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
