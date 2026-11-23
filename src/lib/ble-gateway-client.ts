// ---------------------------------------------------------------------------
// ble-gateway client helper — COPY THIS FILE INTO YOUR OWN ADAPTER.
//
// It lets any ioBroker adapter receive BLE advertisement packets from the
// central `ble-gateway` adapter without owning the HCI controller itself.
//
// Usage in your adapter:
//
//   import { BleGatewayClient } from './lib/ble-gateway-client';
//
//   class MyAdapter extends Adapter {
//       private ble = new BleGatewayClient(this);
//
//       constructor(options) {
//           super({
//               ...options,
//               message:     obj          => this.onMessage(obj),
//               stateChange: (id, state)  => this.ble.handleStateChange(id, state),
//               unload:      cb           => this.onUnload(cb),
//               ready:       ()           => this.onReady(),
//           });
//       }
//
//       async onReady() {
//           await this.ble.subscribe('sensors', { macs: ['aa:bb:cc:dd:ee:ff'] }, packet => {
//               this.log.info(`${packet.address} rssi=${packet.rssi} data=${packet.manufacturerData?.toString('hex')}`);
//           });
//       }
//
//       // stateChange must reach ble.handleStateChange(): it watches the gateway's
//       // `alive` state and re-subscribes when the gateway restarts.
//
//       onMessage(obj) {
//           if (this.ble.handleMessage(obj)) return; // consumed a 'blePacket'
//           // ... your own commands
//       }
//
//       onUnload(cb) {
//           this.ble.destroy().finally(cb);
//       }
//   }
//
// GATT (connect / read / write / notify) through the gateway:
//
//   const conn = await this.ble.gattConnect('aa:bb:cc:dd:ee:ff');
//   try {
//       await conn.subscribe('fff1', data => this.log.info(`notify ${data.toString('hex')}`));
//       await conn.write('fff2', Buffer.from('0102', 'hex'));
//       const value = await conn.read('2a19');
//   } finally {
//       await conn.disconnect();
//   }
//
// `handleMessage` must be called for every message: notifications arrive as
// 'gattNotify', unexpected link loss as 'gattDisconnected' (see onDisconnect).
// ---------------------------------------------------------------------------

export interface BleGatewayFilter {
	macs?: string[];
	serviceUuids?: string[];
	namePrefixes?: string[];
	manufacturerId?: number;
}

/** Advertisement packet handed to your callback. Binary fields are Buffers. */
export interface BleGatewayPacket {
	subscriptionId: string;
	address: string;
	addressType: string;
	rssi: number;
	ts: number;
	/** Whether the device accepts connections (advertising type). */
	connectable?: boolean;
	localName?: string;
	txPowerLevel?: number;
	serviceUuids?: string[];
	manufacturerData?: Buffer;
	serviceData?: { uuid: string; data: Buffer }[];
}

type PacketHandler = (packet: BleGatewayPacket) => void;

interface RawPacket {
	subscriptionId: string;
	address: string;
	addressType: string;
	rssi: number;
	ts: number;
	connectable?: boolean;
	localName?: string;
	txPowerLevel?: number;
	serviceUuids?: string[];
	manufacturerData?: string;
	serviceData?: { uuid: string; data: string }[];
}

interface Entry {
	filter: BleGatewayFilter;
	minIntervalMs?: number;
	handler: PacketHandler;
}

/** A characteristic of a connected device. UUIDs are lowercase, short form where possible ("fff1"). */
export interface BleGattCharacteristic {
	serviceUuid: string;
	uuid: string;
	/** e.g. ["read", "write", "writeWithoutResponse", "notify"] */
	properties: string[];
}

export interface BleGattConnectOptions {
	/** How long the device may take to show up in the scan (default 15000 ms). */
	discoverTimeoutMs?: number;
	/** How long the link may take to come up (default 15000 ms). */
	connectTimeoutMs?: number;
}

// Minimal slice of the ioBroker Adapter API this helper needs.
interface AdapterLike {
	log: {
		info(m: string): void;
		warn(m: string): void;
		error(m: string): void;
		debug(m: string): void;
	};
	sendTo(
		instance: string,
		command: string,
		message: unknown,
		callback?: (result: unknown) => void,
		options?: { timeout?: number },
	): void;
	subscribeForeignStatesAsync(pattern: string): Promise<void>;
	unsubscribeForeignStatesAsync(pattern: string): Promise<void>;
	getForeignStateAsync(
		id: string,
	): Promise<{ val: unknown } | null | undefined>;
}

// Same canonical form as the gateway: "0000FFF1-0000-1000-8000-00805F9B34FB" → "fff1".
function normalizeUuid(u: string): string {
	const s = (u || "").toLowerCase().replace(/[^0-9a-f]/g, "");
	if (
		s.length === 32 &&
		s.startsWith("0000") &&
		s.endsWith("00001000800000805f9b34fb")
	) {
		return s.slice(4, 8);
	}
	return s;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 10000;
// The gateway may wait up to 15 s for the controller to power on before it answers a subscribe.
const SUBSCRIBE_TIMEOUT_MS = 20000;
// Delay after the gateway's `alive` turned true, so it can finish its start-up first.
const RESUBSCRIBE_DELAY_MS = 2000;
// Delay before a subscribe that got no answer is sent again.
const RETRY_DELAY_MS = 10000;

/** Rejection of `request()` when the gateway did not answer in time. */
export class BleGatewayTimeoutError extends Error {}

/** An open GATT connection held by the gateway on behalf of this adapter. */
export class BleGattConnection {
	public readonly id: string;
	public readonly address: string;
	public readonly characteristics: BleGattCharacteristic[];
	private readonly client: BleGatewayClient;
	private readonly notifyHandlers = new Map<string, (data: Buffer) => void>();
	private readonly disconnectHandlers: ((reason: string) => void)[] = [];
	private closed = false;

	// Internal: created by BleGatewayClient.gattConnect().
	public constructor(
		client: BleGatewayClient,
		id: string,
		address: string,
		characteristics: BleGattCharacteristic[],
	) {
		this.client = client;
		this.id = id;
		this.address = address;
		this.characteristics = characteristics;
	}

	public isOpen(): boolean {
		return !this.closed;
	}

	/** Find a characteristic by UUID (short or long form). */
	public findCharacteristic(uuid: string): BleGattCharacteristic | undefined {
		const want = normalizeUuid(uuid);
		return this.characteristics.find((c) => c.uuid === want);
	}

	/** Enable notifications; `onData` receives every notified value. */
	public async subscribe(
		characteristic: string,
		onData: (data: Buffer) => void,
		service?: string,
	): Promise<void> {
		this.notifyHandlers.set(this.key(characteristic, service), onData);
		await this.client.request("gattSubscribe", {
			connectionId: this.id,
			characteristic,
			service,
		});
	}

	public async unsubscribe(
		characteristic: string,
		service?: string,
	): Promise<void> {
		this.notifyHandlers.delete(this.key(characteristic, service));
		await this.client.request("gattUnsubscribe", {
			connectionId: this.id,
			characteristic,
			service,
		});
	}

	/** Write a value. `withoutResponse` = write command instead of write request. */
	public async write(
		characteristic: string,
		data: Buffer,
		withoutResponse = false,
		service?: string,
	): Promise<void> {
		await this.client.request("gattWrite", {
			connectionId: this.id,
			characteristic,
			service,
			data: data.toString("hex"),
			withoutResponse,
		});
	}

	public async read(
		characteristic: string,
		service?: string,
	): Promise<Buffer> {
		const r = await this.client.request<{ data?: string }>("gattRead", {
			connectionId: this.id,
			characteristic,
			service,
		});
		return Buffer.from(r?.data || "", "hex");
	}

	/** Close the link. Safe to call twice or after the link dropped. */
	public async disconnect(): Promise<void> {
		if (this.closed) {
			return;
		}
		this.closed = true;
		this.client.forget(this.id);
		try {
			await this.client.request("gattDisconnect", {
				connectionId: this.id,
			});
		} catch {
			// already gone on the gateway side
		}
	}

	/** Called when the link drops without disconnect() (device gone, gateway restarted, ...). */
	public onDisconnect(cb: (reason: string) => void): void {
		this.disconnectHandlers.push(cb);
	}

	// Internal: called by BleGatewayClient.handleMessage().
	public dispatchNotify(
		characteristic: string,
		service: string,
		data: Buffer,
	): void {
		const uuid = normalizeUuid(characteristic);
		const handler =
			this.notifyHandlers.get(`${normalizeUuid(service)}/${uuid}`) ||
			this.notifyHandlers.get(uuid);
		handler?.(data);
	}

	// Internal: called by BleGatewayClient.handleMessage().
	public dispatchDisconnect(reason: string): void {
		if (this.closed) {
			return;
		}
		this.closed = true;
		for (const cb of this.disconnectHandlers) {
			try {
				cb(reason);
			} catch {
				// ignore
			}
		}
	}

	private key(characteristic: string, service?: string): string {
		const uuid = normalizeUuid(characteristic);
		return service ? `${normalizeUuid(service)}/${uuid}` : uuid;
	}
}

export class BleGatewayClient {
	private readonly adapter: AdapterLike;
	private readonly gateway: string;
	private readonly aliveId: string;
	private readonly onConnectionChange?: (connected: boolean) => void;
	private readonly entries = new Map<string, Entry>();
	private readonly gattConnections = new Map<string, BleGattConnection>();
	// subscription ids waiting to be (re)sent to the gateway
	private readonly pending = new Set<string>();
	private flushTimer: ReturnType<typeof setTimeout> | null = null;
	private watching: Promise<void> | null = null;
	private aliveKnown = false;
	private gatewayAlive = false;
	private connected = false;
	private destroyed = false;

	/**
	 * @param adapter your adapter instance (`this`)
	 * @param gatewayInstance gateway instance id, default "ble-gateway.0"
	 * @param onConnectionChange optional, called with true once the gateway acknowledged a subscription
	 *     and with false when the gateway stops or a subscription fails
	 */
	public constructor(
		adapter: AdapterLike,
		gatewayInstance = "ble-gateway.0",
		onConnectionChange?: (connected: boolean) => void,
	) {
		this.adapter = adapter;
		this.gateway = gatewayInstance;
		this.aliveId = `system.adapter.${this.gateway}.alive`;
		this.onConnectionChange = onConnectionChange;
	}

	/** True while the gateway instance is running (its `alive` state is true). */
	public isGatewayAlive(): boolean {
		return this.gatewayAlive;
	}

	/**
	 * Register (or replace) a subscription and start receiving packets.
	 * If the gateway is not running, the subscription is sent as soon as it starts.
	 * Re-subscribes automatically if the gateway restarts.
	 */
	public async subscribe(
		subscriptionId: string,
		filter: BleGatewayFilter,
		onPacket: PacketHandler,
		minIntervalMs?: number,
	): Promise<void> {
		this.entries.set(subscriptionId, {
			filter,
			minIntervalMs,
			handler: onPacket,
		});
		await this.ensureWatching();
		if (!this.gatewayAlive) {
			this.adapter.log.warn(
				`${this.gateway} is not running — subscription '${subscriptionId}' is sent when it starts`,
			);
			return;
		}
		await this.sendSubscribe(subscriptionId);
	}

	/** Remove one subscription (or all when no id is given). */
	public async unsubscribe(subscriptionId?: string): Promise<void> {
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
				subscriptionId ? { subscriptionId } : {},
			);
		}
	}

	/**
	 * Open a GATT connection to a device through the gateway. The device must
	 * advertise within `discoverTimeoutMs`. Always call `disconnect()` when done.
	 */
	public async gattConnect(
		address: string,
		opts: BleGattConnectOptions = {},
	): Promise<BleGattConnection> {
		// Watch the gateway, so its connections are reported as lost when it stops.
		await this.ensureWatching();
		if (!this.gatewayAlive) {
			throw new Error(`${this.gateway} is not running`);
		}
		const discoverTimeoutMs = opts.discoverTimeoutMs ?? 15000;
		const connectTimeoutMs = opts.connectTimeoutMs ?? 15000;
		const r = await this.request<{
			connectionId: string;
			address: string;
			characteristics: BleGattCharacteristic[];
		}>(
			"gattConnect",
			{
				address: address.toLowerCase(),
				discoverTimeoutMs,
				connectTimeoutMs,
			},
			// discovery + link + service discovery (10 s on the gateway) + margin
			discoverTimeoutMs + connectTimeoutMs + 15000,
		);
		const conn = new BleGattConnection(
			this,
			r.connectionId,
			r.address,
			r.characteristics || [],
		);
		this.gattConnections.set(conn.id, conn);
		return conn;
	}

	/**
	 * Send a command to the gateway and wait for its answer. Rejects if the
	 * gateway answers `{ error }` or does not answer within `timeoutMs`.
	 */
	public request<T = Record<string, unknown>>(
		command: string,
		message: unknown,
		timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
	): Promise<T> {
		return new Promise<T>((resolve, reject) => {
			let settled = false;
			const timer = setTimeout(() => {
				if (!settled) {
					settled = true;
					reject(
						new BleGatewayTimeoutError(
							`${this.gateway} did not answer ${command} within ${timeoutMs} ms`,
						),
					);
				}
			}, timeoutMs);
			this.adapter.sendTo(
				this.gateway,
				command,
				message,
				(res: unknown) => {
					if (settled) {
						return;
					}
					settled = true;
					clearTimeout(timer);
					const r = res as { error?: unknown } | null | undefined;
					if (res instanceof Error) {
						reject(res);
					} else if (r && typeof r === "object" && r.error) {
						reject(
							new Error(
								typeof r.error === "string"
									? r.error
									: JSON.stringify(r.error),
							),
						);
					} else {
						resolve(res as T);
					}
				},
				{ timeout: timeoutMs + 1000 },
			);
		});
	}

	// Internal: a connection closed itself.
	public forget(connectionId: string): void {
		this.gattConnections.delete(connectionId);
	}

	/** Call from your adapter's `message` handler. Returns true if the message was for this helper. */
	public handleMessage(obj: ioBroker.Message | undefined): boolean {
		if (!obj) {
			return false;
		}
		switch (obj.command) {
			case "blePacket": {
				const raw = obj.message as RawPacket;
				const entry = this.entries.get(raw.subscriptionId);
				if (entry) {
					entry.handler(this.decode(raw));
				}
				return true; // ours by command, even without a live handler
			}
			case "gattNotify": {
				const m = obj.message as {
					connectionId: string;
					service: string;
					characteristic: string;
					data: string;
				};
				this.gattConnections
					.get(m?.connectionId)
					?.dispatchNotify(
						m.characteristic,
						m.service || "",
						Buffer.from(m.data || "", "hex"),
					);
				return true;
			}
			case "gattDisconnected": {
				const m = obj.message as {
					connectionId: string;
					reason?: string;
				};
				const conn = this.gattConnections.get(m?.connectionId);
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

	/**
	 * Call from your adapter's `stateChange` handler. Watches the gateway's `alive` state:
	 * the gateway forgets all subscriptions and GATT connections when it restarts, so
	 * subscriptions are sent again as soon as it is back and open connections are
	 * reported via `onDisconnect`.
	 */
	public handleStateChange(
		id: string,
		state: ioBroker.State | null | undefined,
	): void {
		if (id !== this.aliveId || !this.aliveKnown || this.destroyed) {
			return;
		}
		// `alive` is refreshed periodically (true → true) and deleted (null) when it expires after a crash.
		const alive = !!state?.val;
		if (alive === this.gatewayAlive) {
			return;
		}
		this.gatewayAlive = alive;
		if (alive) {
			if (this.entries.size) {
				this.adapter.log.info(
					`${this.gateway} started — re-subscribing`,
				);
				this.clearFlushTimer();
				this.queueSubscribe(this.entries.keys(), RESUBSCRIBE_DELAY_MS);
			}
		} else {
			this.adapter.log.warn(
				`${this.gateway} stopped — waiting for it to come back`,
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

	/** Unsubscribe everything, close all GATT connections and stop watching the gateway. Call from onUnload. */
	public async destroy(): Promise<void> {
		this.destroyed = true;
		this.clearFlushTimer();
		this.pending.clear();
		// An "unsubscribe all" makes the gateway close our GATT connections too.
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
				// ignore
			}
		}
	}

	// --- internals ---------------------------------------------------------

	private ensureWatching(): Promise<void> {
		this.watching ||= (async () => {
			try {
				await this.adapter.subscribeForeignStatesAsync(this.aliveId);
				const st = await this.adapter.getForeignStateAsync(
					this.aliveId,
				);
				this.gatewayAlive = !!st?.val;
			} catch (e) {
				// Unknown state: try anyway, a missing gateway shows up as a timeout.
				this.adapter.log.debug(
					`${this.gateway} watch failed: ${(e as Error).message}`,
				);
				this.gatewayAlive = true;
			}
			this.aliveKnown = true;
		})();
		return this.watching;
	}

	private setConnected(connected: boolean): void {
		if (connected !== this.connected) {
			this.connected = connected;
			this.onConnectionChange?.(connected);
		}
	}

	private clearFlushTimer(): void {
		if (this.flushTimer) {
			clearTimeout(this.flushTimer);
			this.flushTimer = null;
		}
	}

	/** Send the given subscriptions after `delayMs` (joins an already scheduled send). */
	private queueSubscribe(ids: Iterable<string>, delayMs: number): void {
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
			const ids = [...this.pending];
			this.pending.clear();
			for (const id of ids) {
				void this.sendSubscribe(id);
			}
		}, delayMs);
	}

	private async sendSubscribe(subscriptionId: string): Promise<void> {
		const entry = this.entries.get(subscriptionId);
		if (!entry) {
			return;
		}
		let error: unknown = null;
		try {
			await this.request(
				"subscribe",
				{
					subscriptionId,
					filter: entry.filter,
					minIntervalMs: entry.minIntervalMs,
				},
				SUBSCRIBE_TIMEOUT_MS,
			);
		} catch (e) {
			error = e;
		}
		if (this.destroyed || this.entries.get(subscriptionId) !== entry) {
			return; // unsubscribed or replaced meanwhile
		}
		if (!error) {
			this.setConnected(true);
			return;
		}
		this.setConnected(false);
		if (!this.gatewayAlive) {
			return; // the gateway stopped meanwhile — sent again when it is back
		}
		if (error instanceof BleGatewayTimeoutError) {
			// The gateway may still be starting up — try again.
			this.adapter.log.warn(
				`${this.gateway} did not answer subscribe '${subscriptionId}' — retrying`,
			);
			this.queueSubscribe([subscriptionId], RETRY_DELAY_MS);
		} else {
			this.adapter.log.warn(
				`${this.gateway} subscribe '${subscriptionId}' failed: ${(error as Error).message}`,
			);
		}
	}

	// Fire a command and wait for the answer, but never hang (gateway may be down).
	private async send(command: string, message: unknown): Promise<void> {
		try {
			await this.request(command, message, 5000);
		} catch (e) {
			this.adapter.log.debug(
				`${this.gateway} ${command}: ${(e as Error).message}`,
			);
		}
	}

	private decode(raw: RawPacket): BleGatewayPacket {
		const out: BleGatewayPacket = {
			subscriptionId: raw.subscriptionId,
			address: raw.address,
			addressType: raw.addressType,
			rssi: raw.rssi,
			ts: raw.ts,
			connectable: raw.connectable,
			localName: raw.localName,
			txPowerLevel: raw.txPowerLevel,
			serviceUuids: raw.serviceUuids,
		};
		if (raw.manufacturerData) {
			out.manufacturerData = Buffer.from(raw.manufacturerData, "hex");
		}
		if (raw.serviceData) {
			out.serviceData = raw.serviceData.map((sd) => ({
				uuid: sd.uuid,
				data: Buffer.from(sd.data, "hex"),
			}));
		}
		return out;
	}
}
