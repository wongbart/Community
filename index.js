import express from 'express';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Server } from 'socket.io';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { availableParallelism } from 'node:os';
import cluster from 'node:cluster';
import { createAdapter, setupPrimary } from '@socket.io/cluster-adapter';

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i
    });
  }

  setupPrimary();
} else {
  const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_offset TEXT UNIQUE,
      content TEXT,
      nickname TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_client_offset ON messages (client_offset);
  `);

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
    adapter: createAdapter(),
    // Enable compression
    perMessageDeflate: {
      zlibDeflateOptions: {
        chunkSize: 1024,
        memLevel: 7,
        level: 3
      },
      zlibInflateOptions: {
        chunkSize: 10 * 1024
      }
    }
  });

  const __dirname = dirname(fileURLToPath(import.meta.url));

  app.get('/', (req, res) => {
    res.sendFile(join(__dirname, 'index.html'));
  });

  io.on('connection', async (socket) => {
    socket.on('set nickname', (nickname) => {
      socket.nickname = nickname;
    });

    socket.on('chat message', async (msg, clientOffset, callback) => {
      console.time('db-insert');
      let result;
      try {
        result = await db.run('INSERT INTO messages (content, client_offset, nickname) VALUES (?, ?, ?)', msg, clientOffset, socket.nickname || 'Anonymous');
      } catch (e) {
        console.timeEnd('db-insert');
        if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
          callback();
        } else {
          // let the client retry
        }
        return;
      }
      console.timeEnd('db-insert');

      console.time('emit-message');
      io.emit('chat message', { message: msg, nickname: socket.nickname || 'Anonymous' }, result.lastID);
      console.timeEnd('emit-message');
      
      callback();
    });

    if (!socket.recovered) {
      try {
        const rows = await db.all('SELECT id, content, nickname FROM messages WHERE id > ?', [socket.handshake.auth.serverOffset || 0]);
        const messages = rows.map(row => ({ message: row.content, nickname: row.nickname, id: row.id }));
        socket.emit('batch chat messages', messages);
      } catch (e) {
        // handle error
      }
    }
  });

  const port = process.env.PORT;

  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`);
  });
}
