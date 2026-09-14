/**
 * Vue3 组合式函数：在组件里接入中转服务。
 *
 * SDK 文件两种引入方式：
 *   1）拷走 src/relay/static/relay-client.js 放进你的前端工程（推荐）
 *   2）直接从服务端拉：import('http://<server>:8770/sdk/relay-client.js')
 *
 * 用法：
 *   <script setup>
 *   import { useRelay } from './useRelay'
 *
 *   const { connected, clients, lastMessage, call, send, broadcast } = useRelay({
 *     url: 'ws://127.0.0.1:8770/ws',
 *     clientId: 'ui-1',
 *     groups: ['ui'],
 *     // Python/shell 那边 call 过来时由这里处理，返回值自动回传
 *     onRequest: async (env) => {
 *       if (env.payload.cmd === 'render') {
 *         await doRender(env.payload)
 *         return { ok: true }
 *       }
 *       return { ok: false, reason: 'unknown cmd' }
 *     },
 *   })
 *   </script>
 */
import { ref, shallowRef, onMounted, onUnmounted } from 'vue';
import { RelayClient } from '../../src/relay/static/relay-client.js';

export function useRelay(options = {}) {
  const connected = ref(false);
  const state = ref('idle');
  const clients = ref([]);
  const lastMessage = shallowRef(null);
  const messages = ref([]);
  const error = ref(null);

  const client = new RelayClient({
    url: options.url || 'ws://127.0.0.1:8770/ws',
    clientId: options.clientId,
    token: options.token,
    groups: options.groups || [],
    meta: options.meta || {},
  });

  client.onState((s, detail) => {
    state.value = s;
    connected.value = s === 'connected';
    if (s === 'connected') refresh();
    if (s === 'error') error.value = detail;
  });

  client.onRequest(async (env) => {
    lastMessage.value = env;
    messages.value.push(env);
    if (options.onRequest) return await options.onRequest(env);
    return { ok: false, reason: 'no handler' };
  });

  client.onMessage((env) => {
    lastMessage.value = env;
    messages.value.push(env);
    options.onMessage?.(env);
  });

  client.onBroadcast((env) => {
    lastMessage.value = env;
    messages.value.push(env);
    options.onBroadcast?.(env);
  });

  client.onPresence((env) => {
    options.onPresence?.(env);
    refresh();
  });

  async function refresh() {
    try {
      const data = await client.listClients();
      clients.value = data.clients || [];
    } catch (e) {
      /* 忽略 */
    }
  }

  onMounted(() => {
    if (options.autoConnect !== false) {
      client.connect().catch((e) => { error.value = e; });
    }
  });

  onUnmounted(() => client.close());

  return {
    client,
    connected,
    state,
    clients,
    messages,
    lastMessage,
    error,
    refresh,
    connect: () => client.connect(),
    close: () => client.close(),
    send: (to, payload) => client.send(to, payload),
    call: (to, payload, timeout) => client.call(to, payload, timeout),
    broadcast: (payload, group) => client.broadcast(payload, group),
    subscribe: (...g) => client.subscribe(...g),
  };
}
