import { readCar as createCarIterator } from "@atcute/car";
import { decode, decodeFirst, fromBytes, toCidLink } from "@atcute/cbor";
import type { At, ComAtprotoSyncSubscribeRepos } from "@atcute/client/lexicons";

import { createNanoEvents, type Unsubscribe } from "nanoevents";
import type { Data as WSData, WebSocket as WSWebSocket } from "ws";

/**
 * Options for the Firehose class.
 */
export interface FirehoseOptions {
	/**
	 * The Relay to connect to.
	 */
	relay?: string;
	/**
	 * The cursor to listen from. If not provided, the firehose will start from the latest event.
	 */
	cursor?: string;
	/**
	 * Whether to automatically reconnect when no new messages are received for a period of time.
	 * This will not reconnect if the connection was closed intentionally.
	 * To do that, listen for the `"close"` event and call `start()` again.
	 * @default true
	 */
	autoReconnect?: boolean;

	/**
	 * The WebSocket implementation to use (e.g. `import ws from "ws"`).
	 * Not required if you are on Node 21.0.0 or newer, or another environment that provides a WebSocket implementation.
	 */
	ws?: typeof WSWebSocket | typeof WebSocket;
}

export class Firehose {
	/** The relay to connect to. */
	public relay: string;

	/** WebSocket connection to the relay. */
	public ws: WebSocket;

	/** The current cursor. */
	public cursor = "";

	private emitter = createNanoEvents();

	private autoReconnect: boolean;

	private reconnectTimeout: NodeJS.Timeout | undefined;

	/**
	 * Creates a new Firehose instance.
	 * @param options Optional configuration.
	 */
	constructor(options: FirehoseOptions = {}) {
		this.relay = options.relay ?? "wss://bsky.network";
		this.cursor = options.cursor ?? "";
		this.autoReconnect = options.autoReconnect ?? true;

		const cursorQueryParameter = this.cursor ? `?cursor=${this.cursor}` : "";

		if (typeof globalThis.WebSocket === "undefined" && !options.ws) {
			throw new Error(
				`No WebSocket implementation was found in your environment. You must provide an implementation as the \`ws\` option.

For example, in a Node.js environment, \`npm install ws\` and then:
import { Firehose } from "@skyware/firehose";
import WebSocket from "ws";

const firehose = new Firehose({
	ws: WebSocket,
});`,
			);
		}

		const wsImpl = options.ws ?? globalThis.WebSocket;

		// @ts-expect-error — prototype incompatibility
		this.ws = new wsImpl(
			`${this.relay}/xrpc/com.atproto.sync.subscribeRepos${cursorQueryParameter}`,
		);
	}

	/**
	 * Opens a WebSocket connection to the relay.
	 */
	start() {
		this.ws.addEventListener("open", () => {
			this.emitter.emit("open");
		});

		this.ws.addEventListener("message", async ({ data }) => {
			try {
				// eslint-disable-next-line @typescript-eslint/no-unsafe-argument
				const message = await this.parseMessage(data);
				if ("seq" in message && message.seq && !isNaN(message.seq)) {
					this.cursor = `${message.seq}`;
				}
				switch (message.$type) {
					case "com.atproto.sync.subscribeRepos#identity":
						this.emitter.emit("identity", message);
						break;
					case "com.atproto.sync.subscribeRepos#account":
						this.emitter.emit("account", message);
						break;
					case "com.atproto.sync.subscribeRepos#info":
						this.emitter.emit("info", message);
						break;
					case "com.atproto.sync.subscribeRepos#sync":
						this.emitter.emit("sync", message);
						break;
					case "com.atproto.sync.subscribeRepos#commit":
						this.emitter.emit("commit", message);
						break;
					default:
						this.emitter.emit("unknown", message);
						break;
				}
			} catch (error) {
				this.emitter.emit("error", { cursor: this.cursor, error });
			} finally {
				if (this.autoReconnect) this.preventReconnect();
			}
		});

		this.ws.addEventListener("close", () => {
			this.emitter.emit("close", this.cursor);
		});

		this.ws.addEventListener("error", (error) => {
			this.emitter.emit("websocketError", { cursor: this.cursor, error });
		});
	}

	/**
	 * Closes the WebSocket connection.
	 */
	close() {
		this.ws?.close();
	}

	/** Emitted when the connection is opened. */
	on(event: "open", listener: () => void): Unsubscribe;
	/** Emitted when the connection is closed. */
	on(event: "close", listener: (cursor: string) => void): Unsubscribe;
	/**
	 * Emitted when the websocket reconnects due to not receiving any messages for a period of time.
	 * This will only be emitted if the `autoReconnect` option is `true`.
	 */
	on(event: "reconnect", listener: () => void): Unsubscribe;
	/** Emitted when an error occurs while handling a message. */
	on(
		event: "error",
		listener: ({ cursor, error }: { cursor: string; error: Error }) => void,
	): Unsubscribe;
	/** Emitted when an error occurs within the websocket. */
	on(
		event: "websocketError",
		listener: ({ cursor, error }: { cursor: string; error: unknown }) => void,
	): Unsubscribe;
	/** Represents a commit to a user's repository. */
	on(event: "commit", listener: (message: CommitEvent) => void): Unsubscribe;
	/**
	 * Updates the repo to a new state, without necessarily including that state on the firehose.
	 * Used to recover from broken commit streams, data loss incidents, or in situations where upstream
	 * host does not know recent state of the repository.
	 */
	on(event: "sync", listener: (message: SyncEvent) => void): Unsubscribe;
	/** Represents a change to an account's status on a host (eg, PDS or Relay). */
	on(event: "account", listener: (message: AccountEvent) => void): Unsubscribe;
	/**
	 * Represents a change to an account's identity.
	 * Could be an updated handle, signing key, or pds hosting endpoint.
	 */
	on(event: "identity", listener: (message: IdentityEvent) => void): Unsubscribe;
	/** An informational message from the relay. */
	on(event: "info", listener: (message: InfoEvent) => void): Unsubscribe;
	/** Emitted when an unknown message is received. */
	on(event: "unknown", listener: (message: unknown) => void): Unsubscribe;
	on(event: string, listener: (...args: any[]) => void): () => void {
		return this.emitter.on(event, listener);
	}

	private async parseMessage(data: WSData): Promise<Event | { $type: string; seq?: number }> {
		const buffer = new Uint8Array(
			await (new Blob(Array.isArray(data) ? data : [data])).arrayBuffer(),
		);

		const [header, remainder] = decodeFirst(buffer);
		const [body, remainder2] = decodeFirst(remainder);
		if (remainder2.length > 0) {
			throw new Error("Excess bytes in message");
		}

		const { t, op } = parseHeader(header);

		if (op === -1) {
			throw new Error(`Error: ${body.message}\nError code: ${body.error}`);
		}

		if (t === "#commit") {
			const {
				seq,
				repo,
				commit,
				rev,
				since,
				blocks: blocksBytes,
				ops: commitOps,
				prevData,
				time,
			} = body as ComAtprotoSyncSubscribeRepos.Commit;

			// A commit can contain no changes
			if (!blocksBytes?.$bytes?.length) {
				return {
					$type: "com.atproto.sync.subscribeRepos#commit",
					seq,
					repo,
					commit: commit.$link,
					rev,
					since,
					blocks: new Uint8Array(),
					ops: [],
					...(prevData ? { prevData: prevData.$link } : {}),
					time,
				} satisfies CommitEvent;
			}

			const blocks = fromBytes(blocksBytes);
			const car = readCar(blocks);

			const ops: Array<RepoOp> = [];
			for (const op of commitOps) {
				const action: "create" | "update" | "delete" = op.action as any;
				if (action === "create") {
					if (!op.cid) continue;
					const record = car.get(op.cid.$link);
					if (!record) continue;
					ops.push(
						{ action, path: op.path, cid: op.cid.$link, record } satisfies CreateOp,
					);
				} else if (action === "update") {
					if (!op.cid) continue;
					const record = car.get(op.cid.$link);
					if (!record) continue;
					ops.push(
						{
							action,
							path: op.path,
							cid: op.cid.$link,
							...(op.prev ? { prev: op.prev.$link } : {}),
							record,
						} satisfies UpdateOp,
					);
				} else if (action === "delete") {
					ops.push(
						{
							action,
							path: op.path,
							...(op.prev ? { prev: op.prev.$link } : {}),
						} satisfies DeleteOp,
					);
				} else {
					throw new Error(`Unknown action: ${action}`);
				}
			}

			return {
				$type: "com.atproto.sync.subscribeRepos#commit",
				seq,
				repo,
				commit: commit.$link,
				rev,
				since,
				blocks,
				ops,
				...(prevData ? { prevData: prevData.$link } : {}),
				time,
			} satisfies CommitEvent;
		} else if (t === "#sync") {
			const { seq, did, blocks: blocksBytes, rev, time } =
				body as ComAtprotoSyncSubscribeRepos.Sync;

			const blocks = (blocksBytes?.$bytes?.length)
				? fromBytes(blocksBytes)
				: new Uint8Array();
			return {
				$type: "com.atproto.sync.subscribeRepos#sync",
				seq,
				did,
				blocks,
				rev,
				time,
			} satisfies SyncEvent;
		}

		return { $type: `com.atproto.sync.subscribeRepos${t}`, ...body };
	}

	private preventReconnect() {
		if (this.reconnectTimeout) clearTimeout(this.reconnectTimeout);
		this.reconnectTimeout = setTimeout(() => {
			this.reconnect();
		}, 5_000);
	}

	private reconnect() {
		this.ws?.close();
		this.start();
		this.emitter.emit("reconnect");
	}
}

/**
 * Represents a `create` repository operation.
 */
export interface CreateOp {
	action: "create";
	/** The record's path in the repository. */
	path: string;
	/** The record's CID. */
	cid: At.Cid;
	/** The record's content. */
	record: {};
}

/**
 * Represents an `update` repository operation.
 */
export interface UpdateOp {
	action: "update";
	/** The record's path in the repository. */
	path: string;
	/** The record's CID. */
	cid: At.Cid;
	/** The previous record CID. */
	prev?: At.Cid;
	/** The record's content. */
	record: {};
}

/**
 * Represents a `delete` repository operation.
 */
export interface DeleteOp {
	action: "delete";
	/** The record's path in the repository. */
	path: string;
	/** The previous record CID. */
	prev?: At.Cid;
}

/** A repository operation. */
export type RepoOp = CreateOp | UpdateOp | DeleteOp;

/**
 * Represents an update of repository state.
 */
export interface CommitEvent {
	$type: "com.atproto.sync.subscribeRepos#commit";
	/** The stream sequence number of this message. */
	seq: number;
	/** The repo this event comes from. */
	repo: At.Did;
	/** Repo commit object CID. */
	commit: At.Cid;
	/** The rev of the emitted commit. Note that this information is also in the commit object included in blocks. */
	rev: string;
	/** The rev of the last emitted commit from this repo (if any). */
	since: string | null;
	/** CAR file containing relevant blocks, as a diff since the previous repo state. */
	blocks: Uint8Array;
	/** List of repo mutation operations in this commit (eg, records created, updated, or deleted). */
	ops: Array<RepoOp>;
	/** The root CID of the MST tree for the previous commit from this repo (indicated by the 'since' revision field in this message). Corresponds to the 'data' field in the repo commit object. */
	prevData?: At.Cid;
	/** Timestamp of when this message was originally broadcast. */
	time: string;
}

export interface SyncEvent {
	$type: "com.atproto.sync.subscribeRepos#sync";
	/** The stream sequence number of this message. */
	seq: number;
	/** The repo this event comes from. */
	did: At.Did;
	/** CAR file containing the commit, as a block. */
	blocks: Uint8Array;
	/** The rev of the commit. */
	rev: string;
	/** Timestamp of when this message was originally broadcast. */
	time: string;
}

export type AccountEvent = ComAtprotoSyncSubscribeRepos.Account & {
	$type: "com.atproto.sync.subscribeRepos#account";
};
export type IdentityEvent = ComAtprotoSyncSubscribeRepos.Identity & {
	$type: "com.atproto.sync.subscribeRepos#identity";
};
export type InfoEvent = ComAtprotoSyncSubscribeRepos.Info & {
	$type: "com.atproto.sync.subscribeRepos#info";
};
export type Event = CommitEvent | SyncEvent | AccountEvent | IdentityEvent | InfoEvent;

function parseHeader(header: any): { t: string; op: 1 | -1 } {
	if (
		!header
		|| typeof header !== "object"
		|| !header.t
		|| typeof header.t !== "string"
		|| !header.op
		|| typeof header.op !== "number"
	) {
		throw new Error("Invalid header received");
	}
	return { t: header.t, op: header.op };
}

function readCar(buffer: Uint8Array): Map<At.Cid, unknown> {
	const records = new Map<At.Cid, unknown>();
	for (const { cid, bytes } of createCarIterator(buffer).iterate()) {
		records.set(toCidLink(cid).$link, decode(bytes));
	}
	return records;
}
