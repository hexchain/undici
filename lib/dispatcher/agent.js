'use strict'

const { InvalidArgumentError, MaxOriginsReachedError } = require('../core/errors')
const { kClients, kRunning, kClose, kDestroy, kDispatch, kUrl } = require('../core/symbols')
const DispatcherBase = require('./dispatcher-base')
const Pool = require('./pool')
const Client = require('./client')
const util = require('../core/util')

const kOnConnect = Symbol('onConnect')
const kOnDisconnect = Symbol('onDisconnect')
const kOnConnectionError = Symbol('onConnectionError')
const kOnDrain = Symbol('onDrain')
const kFactory = Symbol('factory')
const kOptions = Symbol('options')
const kOrigins = Symbol('origins')
const kClientTtl = Symbol('clientTtl')
const kCleanup = Symbol('cleanup')
const kCleanupTimeout = Symbol('cleanupTimeout')
const kIdleClients = Symbol('idleClients')

function defaultFactory (origin, opts) {
  return opts && opts.connections === 1
    ? new Client(origin, opts)
    : new Pool(origin, opts)
}

class Agent extends DispatcherBase {
  constructor ({ factory = defaultFactory, maxOrigins = Infinity, connect, clientTtl = 0, clientTtlResolution = 2000, ...options } = {}) {
    if (typeof factory !== 'function') {
      throw new InvalidArgumentError('factory must be a function.')
    }

    if (connect != null && typeof connect !== 'function' && typeof connect !== 'object') {
      throw new InvalidArgumentError('connect must be a function or an object')
    }

    if (typeof maxOrigins !== 'number' || Number.isNaN(maxOrigins) || maxOrigins <= 0) {
      throw new InvalidArgumentError('maxOrigins must be a number greater than 0')
    }

    if (clientTtl != null) {
      if (typeof clientTtl !== 'number' || Number.isNaN(clientTtl) || clientTtl < 0) {
        throw new InvalidArgumentError('clientTtl must be a non-negative number')
      }
      if (typeof clientTtlResolution !== 'number' || Number.isNaN(clientTtlResolution) || !Number.isFinite(clientTtlResolution) || clientTtlResolution <= 0) {
        throw new InvalidArgumentError('clientTtlResolution must be a finite positive number')
      }
    }

    super()

    if (connect && typeof connect !== 'function') {
      connect = { ...connect }
    }

    this[kOptions] = { ...util.deepClone(options), maxOrigins, connect }
    this[kFactory] = factory
    this[kClients] = new Map()
    this[kOrigins] = new Set()

    if (clientTtl != null && Number.isFinite(clientTtl)) {
      this[kClientTtl] = clientTtl
      if (clientTtl > 0) {
        this[kIdleClients] = new Set()
        this[kCleanupTimeout] = setInterval(this[kCleanup].bind(this), clientTtlResolution).unref()
      }
    }

    this[kOnDrain] = (origin, targets) => {
      this.emit('drain', origin, [this, ...targets])
    }

    this[kOnConnect] = (origin, targets) => {
      this.emit('connect', origin, [this, ...targets])
    }

    this[kOnDisconnect] = (origin, targets, err) => {
      this.emit('disconnect', origin, [this, ...targets], err)
    }

    this[kOnConnectionError] = (origin, targets, err) => {
      this.emit('connectionError', origin, [this, ...targets], err)
    }
  }

  get [kRunning] () {
    let ret = 0
    for (const { dispatcher } of this[kClients].values()) {
      ret += dispatcher[kRunning]
    }
    return ret
  }

  [kDispatch] (opts, handler) {
    let key
    if (opts.origin && (typeof opts.origin === 'string' || opts.origin instanceof URL)) {
      key = String(opts.origin)
    } else {
      throw new InvalidArgumentError('opts.origin must be a non-empty string or URL.')
    }

    if (this[kOrigins].size >= this[kOptions].maxOrigins && !this[kOrigins].has(key)) {
      throw new MaxOriginsReachedError()
    }

    const result = this[kClients].get(key)
    let dispatcher = result && result.dispatcher
    if (!dispatcher) {
      const checkIdleness = (key) => {
        const result = this[kClients].get(key)
        if (!result) return
        result.count -= 1
        if (result.count === 0) {
          if (this[kClientTtl] === 0) {
            this[kClients].delete(key)
            this[kOrigins].delete(key)
            if (!result.dispatcher.destroyed) {
              result.dispatcher.close()
            }
          } else {
            this[kIdleClients]?.add(key)
          }
        }
      }
      dispatcher = this[kFactory](opts.origin, this[kOptions])
        .on('drain', this[kOnDrain])
        .on('connect', this[kOnConnect])
        .on('disconnect', (origin, targets, err) => {
          checkIdleness(key)
          this[kOnDisconnect](origin, targets, err)
        })
        .on('connectionError', (origin, targets, err) => {
          checkIdleness(key)
          this[kOnConnectionError](origin, targets, err)
        })

      this[kClients].set(key, { count: 1, dispatcher, lastUsed: this[kClientTtl] ? Date.now() : undefined })
      this[kOrigins].add(key)
    } else {
      result.count += 1
      result.lastUsed = this[kClientTtl] ? Date.now() : undefined
      this[kIdleClients]?.delete(key)
    }

    return dispatcher.dispatch(opts, handler)
  }

  [kClose] () {
    clearInterval(this[kCleanupTimeout])
    const closePromises = []
    for (const { dispatcher } of this[kClients].values()) {
      closePromises.push(dispatcher.close())
    }
    this[kClients].clear()

    return Promise.all(closePromises)
  }

  [kDestroy] (err) {
    clearInterval(this[kCleanupTimeout])
    const destroyPromises = []
    for (const { dispatcher } of this[kClients].values()) {
      destroyPromises.push(dispatcher.destroy(err))
    }
    this[kClients].clear()

    return Promise.all(destroyPromises)
  }

  [kCleanup] () {
    const now = Date.now()
    for (const key of this[kIdleClients]) {
      const result = this[kClients].get(key)
      if (!result.count && now - result.lastUsed > this[kClientTtl]) {
        if (!result.dispatcher.destroyed) {
          result.dispatcher.close()
        }
        this[kClients].delete(key)
        this[kOrigins].delete(key)
        this[kIdleClients].delete(key)
      }
    }
  }

  get stats () {
    const allClientStats = {}
    for (const { dispatcher } of this[kClients].values()) {
      if (dispatcher.stats) {
        allClientStats[dispatcher[kUrl].origin] = dispatcher.stats
      }
    }
    return allClientStats
  }
}

module.exports = Agent
