"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var ble_gateway_client_exports = {};
__export(ble_gateway_client_exports, {
  BleGatewayClient: () => BleGatewayClient,
  BleGatewayTimeoutError: () => BleGatewayTimeoutError,
  BleGattConnection: () => BleGattConnection
});
module.exports = __toCommonJS(ble_gateway_client_exports);
function normalizeUuid(u) {
  const s = (u || "").toLowerCase().replace(/[^0-9a-f]/g, "");
  if (s.length === 32 && s.startsWith("0000") && s.endsWith("00001000800000805f9b34fb")) {
    return s.slice(4, 8);
  }
  return s;
}
const DEFAULT_REQUEST_TIMEOUT_MS = 1e4;
const SUBSCRIBE_TIMEOUT_MS = 2e4;
const RESUBSCRIBE_DELAY_MS = 2e3;
const RETRY_DELAY_MS = 1e4;
class BleGatewayTimeoutError extends Error {
}
class BleGattConnection {
  id;
  address;
  characteristics;
  client;
  notifyHandlers = /* @__PURE__ */ new Map();
  disconnectHandlers = [];
  closed = false;
  constructor(client, id, address, characteristics) {
    this.client = client;
    this.id = id;
    this.address = address;
    this.characteristics = characteristics;
  }
  isOpen() {
    return !this.closed;
  }
  findCharacteristic(uuid) {
    const want = normalizeUuid(uuid);
    return this.characteristics.find((c) => c.uuid === want);
  }
  async subscribe(characteristic, onData, service) {
    this.notifyHandlers.set(this.key(characteristic, service), onData);
    await this.client.request("gattSubscribe", {
      connectionId: this.id,
      characteristic,
      service
    });
  }
  async unsubscribe(characteristic, service) {
    this.notifyHandlers.delete(this.key(characteristic, service));
    await this.client.request("gattUnsubscribe", {
      connectionId: this.id,
      characteristic,
      service
    });
  }
  async write(characteristic, data, withoutResponse = false, service) {
    await this.client.request("gattWrite", {
      connectionId: this.id,
      characteristic,
      service,
      data: data.toString("hex"),
      withoutResponse
    });
  }
  async read(characteristic, service) {
    const r = await this.client.request("gattRead", {
      connectionId: this.id,
      characteristic,
      service
    });
    return Buffer.from((r == null ? void 0 : r.data) || "", "hex");
  }
  async disconnect() {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.client.forget(this.id);
    try {
      await this.client.request("gattDisconnect", {
        connectionId: this.id
      });
    } catch {
    }
  }
  onDisconnect(cb) {
    this.disconnectHandlers.push(cb);
  }
  dispatchNotify(characteristic, service, data) {
    const uuid = normalizeUuid(characteristic);
    const handler = this.notifyHandlers.get(`${normalizeUuid(service)}/${uuid}`) || this.notifyHandlers.get(uuid);
    handler == null ? void 0 : handler(data);
  }
  dispatchDisconnect(reason) {
    if (this.closed) {
      return;
    }
    this.closed = true;
    for (const cb of this.disconnectHandlers) {
      try {
        cb(reason);
      } catch {
      }
    }
  }
  key(characteristic, service) {
    const uuid = normalizeUuid(characteristic);
    return service ? `${normalizeUuid(service)}/${uuid}` : uuid;
  }
}
class BleGatewayClient {
  adapter;
  gateway;
  aliveId;
  onConnectionChange;
  entries = /* @__PURE__ */ new Map();
  gattConnections = /* @__PURE__ */ new Map();
  pending = /* @__PURE__ */ new Set();
  flushTimer = null;
  watching = null;
  aliveKnown = false;
  gatewayAlive = false;
  connected = false;
  destroyed = false;
  constructor(adapter, gatewayInstance = "ble-gateway.0", onConnectionChange) {
    this.adapter = adapter;
    this.gateway = gatewayInstance;
    this.aliveId = `system.adapter.${this.gateway}.alive`;
    this.onConnectionChange = onConnectionChange;
  }
  isGatewayAlive() {
    return this.gatewayAlive;
  }
  async subscribe(subscriptionId, filter, onPacket, minIntervalMs) {
    this.entries.set(subscriptionId, {
      filter,
      minIntervalMs,
      handler: onPacket
    });
    await this.ensureWatching();
    if (!this.gatewayAlive) {
      this.adapter.log.warn(
        `${this.gateway} is not running \u2014 subscription '${subscriptionId}' is sent when it starts`
      );
      return;
    }
    await this.sendSubscribe(subscriptionId);
  }
  async unsubscribe(subscriptionId) {
    if (subscriptionId) {
      this.entries.delete(subscriptionId);
      this.pending.delete(subscriptionId);
    } else {
      this.entries.clear();
      this.pending.clear();
    }
    if (this.gatewayAlive) {
      await this.send(
        "unsubscribe",
        subscriptionId ? { subscriptionId } : {}
      );
    }
  }
  async gattConnect(address, opts = {}) {
    var _a, _b;
    await this.ensureWatching();
    if (!this.gatewayAlive) {
      throw new Error(`${this.gateway} is not running`);
    }
    const discoverTimeoutMs = (_a = opts.discoverTimeoutMs) != null ? _a : 15e3;
    const connectTimeoutMs = (_b = opts.connectTimeoutMs) != null ? _b : 15e3;
    const r = await this.request(
      "gattConnect",
      {
        address: address.toLowerCase(),
        discoverTimeoutMs,
        connectTimeoutMs
      },
      discoverTimeoutMs + connectTimeoutMs + 15e3
    );
    const conn = new BleGattConnection(
      this,
      r.connectionId,
      r.address,
      r.characteristics || []
    );
    this.gattConnections.set(conn.id, conn);
    return conn;
  }
  request(command, message, timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS) {
    return new Promise((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        if (!settled) {
          settled = true;
          reject(
            new BleGatewayTimeoutError(
              `${this.gateway} did not answer ${command} within ${timeoutMs} ms`
            )
          );
        }
      }, timeoutMs);
      this.adapter.sendTo(
        this.gateway,
        command,
        message,
        (res) => {
          if (settled) {
            return;
          }
          settled = true;
          clearTimeout(timer);
          const r = res;
          if (res instanceof Error) {
            reject(res);
          } else if (r && typeof r === "object" && r.error) {
            reject(
              new Error(
                typeof r.error === "string" ? r.error : JSON.stringify(r.error)
              )
            );
          } else {
            resolve(res);
          }
        },
        { timeout: timeoutMs + 1e3 }
      );
    });
  }
  forget(connectionId) {
    this.gattConnections.delete(connectionId);
  }
  handleMessage(obj) {
    var _a;
    if (!obj) {
      return false;
    }
    switch (obj.command) {
      case "blePacket": {
        const raw = obj.message;
        const entry = this.entries.get(raw.subscriptionId);
        if (entry) {
          entry.handler(this.decode(raw));
        }
        return true;
      }
      case "gattNotify": {
        const m = obj.message;
        (_a = this.gattConnections.get(m == null ? void 0 : m.connectionId)) == null ? void 0 : _a.dispatchNotify(
          m.characteristic,
          m.service || "",
          Buffer.from(m.data || "", "hex")
        );
        return true;
      }
      case "gattDisconnected": {
        const m = obj.message;
        const conn = this.gattConnections.get(m == null ? void 0 : m.connectionId);
        if (conn) {
          this.gattConnections.delete(conn.id);
          conn.dispatchDisconnect(m.reason || "unknown");
        }
        return true;
      }
      default:
        return false;
    }
  }
  handleStateChange(id, state) {
    if (id !== this.aliveId || !this.aliveKnown || this.destroyed) {
      return;
    }
    const alive = !!(state == null ? void 0 : state.val);
    if (alive === this.gatewayAlive) {
      return;
    }
    this.gatewayAlive = alive;
    if (alive) {
      if (this.entries.size) {
        this.adapter.log.info(
          `${this.gateway} started \u2014 re-subscribing`
        );
        this.clearFlushTimer();
        this.queueSubscribe(this.entries.keys(), RESUBSCRIBE_DELAY_MS);
      }
    } else {
      this.adapter.log.warn(
        `${this.gateway} stopped \u2014 waiting for it to come back`
      );
      this.clearFlushTimer();
      this.pending.clear();
      this.setConnected(false);
      const conns = [...this.gattConnections.values()];
      this.gattConnections.clear();
      for (const conn of conns) {
        conn.dispatchDisconnect("gateway stopped");
      }
    }
  }
  async destroy() {
    this.destroyed = true;
    this.clearFlushTimer();
    this.pending.clear();
    if (this.gatewayAlive || !this.aliveKnown) {
      await this.send("unsubscribe", {});
    }
    for (const conn of this.gattConnections.values()) {
      conn.dispatchDisconnect("client destroyed");
    }
    this.gattConnections.clear();
    this.entries.clear();
    if (this.watching) {
      this.watching = null;
      try {
        await this.adapter.unsubscribeForeignStatesAsync(this.aliveId);
      } catch {
      }
    }
  }
  ensureWatching() {
    this.watching || (this.watching = (async () => {
      try {
        await this.adapter.subscribeForeignStatesAsync(this.aliveId);
        const st = await this.adapter.getForeignStateAsync(
          this.aliveId
        );
        this.gatewayAlive = !!(st == null ? void 0 : st.val);
      } catch (e) {
        this.adapter.log.debug(
          `${this.gateway} watch failed: ${e.message}`
        );
        this.gatewayAlive = true;
      }
      this.aliveKnown = true;
    })());
    return this.watching;
  }
  setConnected(connected) {
    var _a;
    if (connected !== this.connected) {
      this.connected = connected;
      (_a = this.onConnectionChange) == null ? void 0 : _a.call(this, connected);
    }
  }
  clearFlushTimer() {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }
  queueSubscribe(ids, delayMs) {
    for (const id of ids) {
      this.pending.add(id);
    }
    if (this.flushTimer || this.destroyed) {
      return;
    }
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      if (!this.gatewayAlive || this.destroyed) {
        return;
      }
      const ids2 = [...this.pending];
      this.pending.clear();
      for (const id of ids2) {
        void this.sendSubscribe(id);
      }
    }, delayMs);
  }
  async sendSubscribe(subscriptionId) {
    const entry = this.entries.get(subscriptionId);
    if (!entry) {
      return;
    }
    let error = null;
    try {
      await this.request(
        "subscribe",
        {
          subscriptionId,
          filter: entry.filter,
          minIntervalMs: entry.minIntervalMs
        },
        SUBSCRIBE_TIMEOUT_MS
      );
    } catch (e) {
      error = e;
    }
    if (this.destroyed || this.entries.get(subscriptionId) !== entry) {
      return;
    }
    if (!error) {
      this.setConnected(true);
      return;
    }
    this.setConnected(false);
    if (!this.gatewayAlive) {
      return;
    }
    if (error instanceof BleGatewayTimeoutError) {
      this.adapter.log.warn(
        `${this.gateway} did not answer subscribe '${subscriptionId}' \u2014 retrying`
      );
      this.queueSubscribe([subscriptionId], RETRY_DELAY_MS);
    } else {
      this.adapter.log.warn(
        `${this.gateway} subscribe '${subscriptionId}' failed: ${error.message}`
      );
    }
  }
  async send(command, message) {
    try {
      await this.request(command, message, 5e3);
    } catch (e) {
      this.adapter.log.debug(
        `${this.gateway} ${command}: ${e.message}`
      );
    }
  }
  decode(raw) {
    const out = {
      subscriptionId: raw.subscriptionId,
      address: raw.address,
      addressType: raw.addressType,
      rssi: raw.rssi,
      ts: raw.ts,
      connectable: raw.connectable,
      localName: raw.localName,
      txPowerLevel: raw.txPowerLevel,
      serviceUuids: raw.serviceUuids
    };
    if (raw.manufacturerData) {
      out.manufacturerData = Buffer.from(raw.manufacturerData, "hex");
    }
    if (raw.serviceData) {
      out.serviceData = raw.serviceData.map((sd) => ({
        uuid: sd.uuid,
        data: Buffer.from(sd.data, "hex")
      }));
    }
    return out;
  }
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  BleGatewayClient,
  BleGatewayTimeoutError,
  BleGattConnection
});
//# sourceMappingURL=ble-gateway-client.js.map
