/**
 * YAgent Commulate Server —— 浏览器 / Vue3 客户端 SDK
 *
 * 用法（Vue3 <script setup>）：
 *   import { RelayClient } from './relay-client.js'
 *
 *   const client = new RelayClient({
 *     url: 'ws://127.0.0.1:8770/ws',
 *     clientId: 'ui-1',
 *     groups: ['ui'],
 *   })
 *
 *   // 别人 call 我时触发，返回值自动回给对方（支持 async）
 *   client.onRequest(async (env) => {
 *     return { ok: true, got: env.payload }
 *   })
 *
 *   client.onMessage((env) => console.log('单向消息', env.payload))
 *   client.onPresence((env) => console.log('上下线', env.payload))
 *   await client.connect()
 *
 *   const result = await client.call('py-worker', { cmd: 'run' }, 15000)
 *
 * 同时支持 ESM import 和 <script src> 直引（挂到 window.RelayClient）。
 */

const newId = () =>
  (crypto?.randomUUID?.() || Math.random().toString(36).slice(2) + Date.now().toString(36)).replace(
    /-/g,
    ''
  );

export class RelayError extends Error {
  constructor(code, message, ref) {
    super(`[${code}] ${message || ''}`);
    this.name = 'RelayError';
    this.code = code;
    this.detail = message;
    this.ref = ref;
  }
}

export class RelayClient {
  /**
   * @param {object} opts
   * @param {string} opts.url          ws 地址，如 ws://127.0.0.1:8770/ws
   * @param {string} opts.clientId     本客户端唯一标识
   * @param {string} [opts.token]      鉴权口令（服务端配置了才需要）
   * @param {string[]} [opts.groups]   加入的分组
   * @param {object} [opts.meta]       附带的元信息
   * @param {boolean} [opts.autoReconnect=true]
   * @param {number} [opts.requestTimeout=30000]  call 的默认超时（毫秒）
   * @param {number} [opts.pingInterval=20000]
   */
  constructor(opts = {}) {
    this.url = opts.url || 'ws://127.0.0.1:8770/ws';
    this.clientId = opts.clientId;
    this.token = opts.token || '';
    this.groups = opts.groups || [];
    this.meta = opts.meta || {};
    this.autoReconnect = opts.autoReconnect !== false;
    this.requestTimeout = opts.requestTimeout || 30000;
    this.pingInterval = opts.pingInterval || 20000;
    this.maxReconnectDelay = opts.maxReconnectDelay || 30000;

    if (!this.clientId) throw new Error('RelayClient: 必须提供 clientId');

    this.ws = null;
    this.connected = false;
    this.sessionId = null;
    this.welcome = null;

    this._pending = new Map();
    this._handlers = { request: null, message: null, broadcast: null, presence: null };
    this._rawHandlers = new Map();
    this._stateHandlers = [];
    this._closedByUser = false;
    this._reconnectDelay = 1000;
    this._pingTimer = null;
    this._connectWaiters = [];
  }

  // ---------------- 处理器 ----------------

  onRequest(fn) { this._handlers.request = fn; return this; }
  onMessage(fn) { this._handlers.message = fn; return this; }
  onBroadcast(fn) { this._handlers.broadcast = fn; return this; }
  onPresence(fn) { this._handlers.presence = fn; return this; }
  onState(fn) { this._stateHandlers.push(fn); return this; }

  on(type, fn) {
    if (!this._rawHandlers.has(type)) this._rawHandlers.set(type, []);
    this._rawHandlers.get(type).push(fn);
    return this;
  }

  _emitState(state, detail) {
    for (const fn of this._stateHandlers) {
      try { fn(state, detail); } catch (e) { console.error(e); }
    }
  }

  // ---------------- 连接 ----------------

  _buildUrl() {
    const q = new URLSearchParams({ client_id: this.clientId });
    if (this.groups.length) q.set('groups', this.groups.join(','));
    if (this.token) q.set('token', this.token);
    return `${this.url}${this.url.includes('?') ? '&' : '?'}${q.toString()}`;
  }

  connect() {
    this._closedByUser = false;
    return new Promise((resolve, reject) => {
      if (this.connected) return resolve(this);
      this._connectWaiters.push({ resolve, reject });
      this._open();
    });
  }

  _open() {
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }
    this._emitState('connecting');
    const ws = new WebSocket(this._buildUrl());
    this.ws = ws;

    ws.onopen = () => {
      this._reconnectDelay = 1000;
      if (Object.keys(this.meta).length) {
        this._rawSend({ type: 'hello', from: this.clientId, payload: { groups: this.groups, meta: this.meta } });
      }
      this._startPing();
    };

    ws.onmessage = (evt) => this._onRaw(evt.data);

    ws.onerror = () => this._emitState('error');

    ws.onclose = (evt) => {
      this.connected = false;
      this._stopPing();
      this._emitState('disconnected', { code: evt.code, reason: evt.reason });

      const err = new RelayError('DISCONNECTED', `连接关闭 code=${evt.code} ${evt.reason || ''}`);
      for (const { reject } of this._connectWaiters.splice(0)) reject(err);
      for (const [, p] of this._pending) {
        clearTimeout(p.timer);
        p.reject(err);
      }
      this._pending.clear();

      if (!this._closedByUser && this.autoReconnect && evt.code !== 4401) {
        setTimeout(() => this._open(), this._reconnectDelay);
        this._reconnectDelay = Math.min(this._reconnectDelay * 2, this.maxReconnectDelay);
      }
    };
  }

  close() {
    this._closedByUser = true;
    this._stopPing();
    if (this.ws) this.ws.close(1000, 'client_close');
    this.connected = false;
  }

  _startPing() {
    this._stopPing();
    this._pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this._rawSend({ type: 'ping', id: newId() });
      }
    }, this.pingInterval);
  }

  _stopPing() {
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
  }

  // ---------------- 收消息 ----------------

  async _onRaw(raw) {
    let env;
    try { env = JSON.parse(raw); } catch { console.warn('非法 JSON', raw); return; }

    for (const fn of this._rawHandlers.get(env.type) || []) {
      try { await fn(env); } catch (e) { console.error(e); }
    }

    switch (env.type) {
      case 'welcome': {
        this.welcome = env.payload || {};
        this.sessionId = this.welcome.session_id;
        this.connected = true;
        this._emitState('connected', this.welcome);
        for (const { resolve } of this._connectWaiters.splice(0)) resolve(this);
        return;
      }
      case 'ping':
        this._rawSend({ type: 'pong', reply_to: env.id });
        return;
      case 'pong':
      case 'ack':
      case 'list_result':
      case 'reply':
        this._resolve(env.reply_to, env);
        return;
      case 'error': {
        const info = env.error || {};
        const ref = env.reply_to || info.ref;
        const err = new RelayError(info.code || 'ERROR', info.message, ref);
        if (ref && this._pending.has(ref)) {
          const p = this._pending.get(ref);
          this._pending.delete(ref);
          clearTimeout(p.timer);
          p.reject(err);
        } else {
          console.warn('relay error:', err.message);
          this._emitState('error', err);
        }
        return;
      }
      case 'forward':
        await this._handleForward(env);
        return;
      case 'broadcast':
        if (this._handlers.broadcast) await this._handlers.broadcast(env);
        return;
      case 'presence':
        if (this._handlers.presence) await this._handlers.presence(env);
        return;
      default:
        console.debug('未处理的消息类型', env.type);
    }
  }

  async _handleForward(env) {
    if (env.expect_reply) {
      const handler = this._handlers.request;
      if (!handler) {
        this._rawSend({
          type: 'reply', id: newId(), reply_to: env.id,
          error: { code: 'NO_HANDLER', message: `'${this.clientId}' 未注册请求处理器` },
        });
        return;
      }
      try {
        const result = await handler(env);
        this._rawSend({ type: 'reply', id: newId(), reply_to: env.id, payload: result ?? null });
      } catch (e) {
        this._rawSend({
          type: 'reply', id: newId(), reply_to: env.id,
          error: { code: 'HANDLER_ERROR', message: String(e && e.message ? e.message : e) },
        });
      }
      return;
    }
    if (this._handlers.message) await this._handlers.message(env);
  }

  _resolve(ref, env) {
    if (!ref || !this._pending.has(ref)) return;
    const p = this._pending.get(ref);
    this._pending.delete(ref);
    clearTimeout(p.timer);
    p.resolve(env);
  }

  // ---------------- 发消息 ----------------

  _rawSend(obj) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new RelayError('NOT_CONNECTED', '尚未连接到中转服务');
    }
    this.ws.send(JSON.stringify(obj));
  }

  _await(msgId, timeout) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(msgId);
        reject(new RelayError('TIMEOUT', `等待响应超时（${timeout}ms）`, msgId));
      }, timeout);
      this._pending.set(msgId, { resolve, reject, timer });
    });
  }

  /** 单向发送；needAck=true 时返回服务端的投递确认 */
  async send(to, payload, { needAck = false } = {}) {
    const id = newId();
    const waiter = needAck ? this._await(id, this.requestTimeout) : null;
    this._rawSend({ type: 'send', id, from: this.clientId, to, payload, need_ack: needAck });
    return waiter ? waiter.then((env) => env.payload) : null;
  }

  /** 请求-响应：发给 to 并等回复，resolve 对方回复的 payload */
  async call(to, payload, timeout) {
    const ms = timeout || this.requestTimeout;
    const id = newId();
    const waiter = this._await(id, ms + 2000);
    this._rawSend({
      type: 'send', id, from: this.clientId, to, payload,
      expect_reply: true, timeout: ms / 1000,
    });
    const env = await waiter;
    if (env.error) throw new RelayError(env.error.code, env.error.message, id);
    return env.payload;
  }

  /** 回复一条 expect_reply 的消息（手动模式，onRequest 会自动做这件事） */
  reply(env, payload) {
    this._rawSend({ type: 'reply', id: newId(), reply_to: env.id, payload });
  }

  broadcast(payload, group = null) {
    this._rawSend({ type: 'broadcast', id: newId(), from: this.clientId, group, payload });
  }

  async subscribe(...groups) {
    return this._control('subscribe', { groups });
  }

  async unsubscribe(...groups) {
    return this._control('unsubscribe', { groups });
  }

  async listClients() {
    const env = await this._control('list', null);
    return env.payload || {};
  }

  async _control(type, payload) {
    const id = newId();
    const waiter = this._await(id, this.requestTimeout);
    this._rawSend({ type, id, from: this.clientId, payload });
    return waiter;
  }
}

if (typeof window !== 'undefined') {
  window.RelayClient = RelayClient;
  window.RelayError = RelayError;
}

export default RelayClient;
