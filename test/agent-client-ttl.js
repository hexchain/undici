const { test } = require('node:test')
const { createServer } = require('node:http')
const { request, Agent } = require('..')
const { once } = require('node:events')
const { setTimeout: sleep } = require('node:timers/promises')

test('Agent should respect clientTtl', async (t) => {
  // use a long keepAliveTimeout to make sure the client closes the connection
  const server = createServer({ joinDuplicateHeaders: true, keepAliveTimeout: 10000 }, (req, res) => {
    res.writeHead(200)
    res.end('hello')
  })

  t.after(() => server.close())

  server.listen()
  await once(server, 'listening')

  const key = `http://localhost:${server.address().port}`
  // under these settings, keep-alive connections will be closed after 300ms, and the client will be destroyed after an
  // extra (500 - 300 = 200)ms.
  const agent = new Agent({
    clientTtl: 500,
    clientTtlResolution: 50,
    keepAliveMaxTimeout: 300,
    keepAliveTimeoutThreshold: 0
  })
  const resp = await request(`http://localhost:${server.address().port}`, { dispatcher: agent })
  t.assert.strictEqual(resp.statusCode, 200)

  await sleep()

  // the client still has a connected socket due to keep-alive
  t.assert.strictEqual(agent.stats[key].connected, 1)

  // the client socket is closed, but the client is still there
  await sleep(350)
  t.assert.strictEqual(agent.stats[key].connected, 0)

  // the client should be destroyed at this moment
  await sleep(200)
  t.assert.strictEqual(Object.keys(agent.stats).length, 0)
})
