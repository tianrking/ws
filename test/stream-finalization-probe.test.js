'use strict';

const assert = require('assert');
const WebSocket = require('..');

describe('Native stream finalization probe', () => {
  for (const side of ['client', 'server']) {
    for (const terminate of [false, true]) {
      it(`${side} ${terminate ? 'terminated regression' : 'graceful control'}`, (done) => {
        let client;
        let duplex;
        const wss = new WebSocket.Server({ port: 0 }, () => {
          client = new WebSocket(`ws://127.0.0.1:${wss.address().port}`);
          if (side === 'client') wrap(client);
          client.on('open', () => client.send('ready'));
        });

        function wrap(ws) {
          const events = [];
          duplex = WebSocket.createWebSocketStream(ws);
          duplex.on('error', (err) => events.push(err.message));
          duplex.on('finish', () => events.push('finish'));
          duplex.on('end', () => events.push('end'));
          duplex.on('close', () => events.push('close'));
          ws.on('message', () => {
            if (terminate) ws.terminate();
            else ws.close();
          });
          ws.on('close', () => {
            assert.strictEqual(ws.readyState, WebSocket.CLOSED);
            console.log(side, 'socket destroyed:', ws._socket.destroyed,
              'socket finished:', ws._socket._writableState.finished);
            duplex.end();
            duplex.resume();
            setImmediate(() => {
              let error;
              try {
                assert.strictEqual(duplex._writableState.finished, true,
                  'closed WebSocket must not leave stream finalization pending');
                assert.strictEqual(duplex.destroyed, true);
                assert.ok(events.includes('finish'));
                assert.ok(events.includes('end'));
                assert.ok(events.includes('close'));
              } catch (err) {
                error = err;
              }
              duplex.destroy();
              client.terminate();
              for (const peer of wss.clients) peer.terminate();
              wss.close(() => done(error));
            });
          });
        }

        wss.on('connection', (peer) => {
          if (side === 'server') wrap(peer);
          peer.on('message', () => {
            if (side === 'client') peer.send('ready');
          });
        });
      });
    }
  }
});
