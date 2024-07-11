import type { ComAtprotoSyncSubscribeRepos } from "@atproto/api";
import { cborToLexRecord, readCar } from "@atproto/repo";
import { Frame } from "@atproto/xrpc-server";
import type { CID } from "multiformats";
import { EventEmitter } from "node:events";
import * as WS from "ws";

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
	 * How frequently to update the stored cursor, in milliseconds.
	 * @default 5000
	 */
	setCursorInterval?: number;
	/**
	 * A function to call whenever the cursor is updated. Useful for storing the cursor in a file or database.
	 */
	onCursorUpdate?: (cursor: string) => void;
	/**
	 * Whether to automatically reconnect when no new messages are received for a period of time.
	 * This will not reconnect if the connection was closed intentionally.
	 * To do that, listen for the `"close"` event and call `start()` again.
	 * @default true
	 */
	autoReconnect?: boolean;
}

export class Firehose extends EventEmitter {
	/** The relay to connect to. */
	public relay: string;

	/** WebSocket connection to the relay. */
	public ws?: WS.WebSocket;

	/** The current cursor. */
	public cursor = "";

	private cursorInterval?: NodeJS.Timeout | undefined;

	private lastCursorUpdate = Date.now();

	private options: Required<
		Pick<FirehoseOptions, "setCursorInterval" | "onCursorUpdate" | "autoReconnect">
	>;

	/**
	 * Creates a new Firehose instance.
	 * @param options Optional configuration.
	 */
	constructor(options: FirehoseOptions = {}) {
		super();
		this.relay = options.relay ?? "wss://bsky.network";
		this.cursor = options.cursor ?? "";
		this.options = {
			setCursorInterval: options.setCursorInterval ?? 5000,
			onCursorUpdate: options.onCursorUpdate ?? (() => {}),
			autoReconnect: options.autoReconnect ?? true,
		};
	}

	/**
	 * Opens a WebSocket connection to the relay.
	 */
	start() {
		const cursorQueryParameter = this.cursor ? `?cursor=${this.cursor}` : "";
		this.ws = new WS.WebSocket(
			`${this.relay}/xrpc/com.atproto.sync.subscribeRepos${cursorQueryParameter}`,
		);

		this.ws.on("open", () => {
			this.emit("open");
		});

		this.ws.on("message", async (data) => {
			try {
				const message = await this.parseMessage(data);
				if ("seq" in message && message.seq && typeof message.seq === "string") {
					this.setCursor(message.seq);
				}
				switch (message.$type) {
					case "com.atproto.sync.subscribeRepos#handle":
						this.emit("handle", message);
						break;
					case "com.atproto.sync.subscribeRepos#tombstone":
						this.emit("tombstone", message);
						break;
					case "com.atproto.sync.subscribeRepos#migrate":
						this.emit("migrate", message);
						break;
					case "com.atproto.sync.subscribeRepos#identity":
						this.emit("identity", message);
						break;
					case "com.atproto.sync.subscribeRepos#info":
						this.emit("info", message);
						break;
					case "com.atproto.sync.subscribeRepos#commit":
						this.emit("commit", message);
						break;
					default:
						this.emit("unknown", message);
						break;
				}
			} catch (error) {
				this.emit("error", { cursor: this.cursor, error });
			}
		});

		this.ws.on("close", () => {
			this.emit("close", this.cursor);
		});

		this.ws.on("error", (error) => {
			this.emit("websocketError", { cursor: this.cursor, error });
		});

		setInterval(() => {
			if (
				this.options.autoReconnect // If the cursor hasn't been updated in twice the interval, reopen the connection
				&& Date.now() - this.lastCursorUpdate > this.options.setCursorInterval * 2
			) {
				// We don't want to emit the close event if we're just reconnecting
				this.ws?.removeAllListeners("close");
				this.ws?.terminate();
				this.start();
				// Instead, we'll emit reconnect
				this.emit("reconnect");
			}
		}, this.options.setCursorInterval);
	}

	/**
	 * Closes the WebSocket connection.
	 */
	close() {
		this.ws?.close();
	}

	/** Emitted when the connection is opened. */
	override on(event: "open", listener: () => void): this;
	/** Emitted when the connection is closed. */
	override on(event: "close", listener: (cursor: string) => void): this;
	/**
	 * Emitted when the websocket reconnects due to not receiving any messages for a period of time.
	 * This will only be emitted if the `autoReconnect` option is `true`.
	 */
	override on(event: "reconnect", listener: () => void): this;
	/** Emitted when an error occurs while handling a message. */
	override on(
		event: "error",
		listener: ({ cursor, error }: { cursor: string; error: Error }) => void,
	): this;
	/** Emitted when an error occurs within the websocket. */
	override on(
		event: "websocketError",
		listener: ({ cursor, error }: { cursor: string; error: unknown }) => void,
	): this;
	/** Emitted when an unknown message is received. */
	override on(event: "unknown", listener: (message: unknown) => void): this;
	/**
	 * Represents an update of an account's handle, or transition to/from invalid state.
	 * NOTE: Will be deprecated in favor of #identity.
	 */
	override on(
		event: "handle",
		listener: (
			message: ComAtprotoSyncSubscribeRepos.Handle & {
				$type: "com.atproto.sync.subscribeRepos#handle";
			},
		) => void,
	): this;
	/**
	 * Represents an account moving from one PDS instance to another.
	 * NOTE: not implemented; account migration uses #identity instead
	 */
	override on(
		event: "migrate",
		listener: (
			message: ComAtprotoSyncSubscribeRepos.Migrate & {
				$type: "com.atproto.sync.subscribeRepos#migrate";
			},
		) => void,
	): this;
	/**
	 * Indicates that an account has been deleted.
	 * NOTE: may be deprecated in favor of #identity or a future #account event
	 */
	override on(
		event: "tombstone",
		listener: (
			message: ComAtprotoSyncSubscribeRepos.Tombstone & {
				$type: "com.atproto.sync.subscribeRepos#tombstone";
			},
		) => void,
	): this;
	/**
	 * Represents a change to an account's identity.
	 * Could be an updated handle, signing key, or pds hosting endpoint.
	 */
	override on(
		event: "identity",
		listener: (
			message: ComAtprotoSyncSubscribeRepos.Identity & {
				$type: "com.atproto.sync.subscribeRepos#identity";
			},
		) => void,
	): this;
	/** Represents a commit to a user's repository. */
	override on(event: "commit", listener: (message: ParsedCommit) => void): this;
	/** An informational message from the relay. */
	override on(
		event: "info",
		listener: (
			message: ComAtprotoSyncSubscribeRepos.Info & {
				$type: "com.atproto.sync.subscribeRepos#info";
			},
		) => void,
	): this;
	override on(event: string, listener: (...args: any[]) => void): this {
		super.on(event, listener);
		return this;
	}

	private async parseMessage(data: WS.RawData) {
		let buffer: Buffer;
		if (data instanceof Buffer) {
			buffer = data;
		} else if (data instanceof ArrayBuffer) {
			buffer = Buffer.from(data);
		} else if (Array.isArray(data)) {
			buffer = Buffer.concat(data);
		} else {
			throw new Error("Unknown message contents: " + data);
		}

		const frame = Frame.fromBytes(buffer);

		if (frame.isError()) throw new Error(`Error: ${frame.message}\nError code: ${frame.code}`);
		if (!frame.header.t || !frame.body || typeof frame.body !== "object") {
			throw new Error("Invalid frame structure: " + JSON.stringify(frame, null, 2));
		}

		if (frame.header.t === "#commit") {
			// A commit can contain no changes
			if (!("blocks" in frame.body) || !(frame.body.blocks instanceof Uint8Array)) {
				return {
					$type: `com.atproto.sync.subscribeRepos#commit`,
					...(frame.body as ComAtprotoSyncSubscribeRepos.Commit),
					ops: [],
				} satisfies ParsedCommit;
			}

			const commit = frame.body as ComAtprotoSyncSubscribeRepos.Commit;
			const car = await readCar(commit.blocks);
			const ops: Array<RepoOp> = commit.ops.map((op) => {
				const action: "create" | "update" | "delete" = op.action as any;
				if (action === "create" || action === "update") {
					if (!op.cid) return;
					const recordBlocks = car.blocks.get(op.cid);
					if (!recordBlocks) return;
					return {
						action,
						path: op.path,
						cid: op.cid,
						record: cborToLexRecord(recordBlocks),
					};
				} else if (action === "delete") {
					return { action, path: op.path };
				} else {
					throw new Error(`Unknown action: ${action}`);
				}
			}).filter((op): op is Exclude<typeof op, undefined> => !!op);
			return {
				$type: "com.atproto.sync.subscribeRepos#commit",
				...commit,
				ops,
			} satisfies ParsedCommit;
		}
		return { $type: `com.atproto.sync.subscribeRepos${frame.header.t}`, ...frame.body };
	}

	/** Sets the cursor once every `setCursorInterval` milliseconds. */
	private setCursor(cursor: string) {
		if (this.cursorInterval) return;
		this.cursorInterval = setTimeout(() => {
			this.cursor = cursor;
			this.cursorInterval = undefined;
			this.lastCursorUpdate = Date.now();
			this.options.onCursorUpdate?.(cursor);
		}, this.options.setCursorInterval);
	}
}

/**
 * Represents a record in a repository. An object with string keys & properties of any type.
 */
export type RepoRecord = ReturnType<typeof cborToLexRecord>;

/**
 * Represents a `create` or `update` repository operation.
 */
export interface CreateOrUpdateOp {
	action: "create" | "update";

	/** The record's path in the repository. */
	path: string;

	/** The record's CID. */
	cid: CID;

	/** The record itself. */
	record: RepoRecord;
}

/**
 * Represents a `delete` repository operation.
 */
export interface DeleteOp {
	action: "delete";

	/** The record's path in the repository. */
	path: string;
}

/** A repository operation. */
export type RepoOp = CreateOrUpdateOp | DeleteOp;

/**
 * Represents an update of repository state.
 */
export interface ParsedCommit {
	$type: "com.atproto.sync.subscribeRepos#commit";
	/** The stream sequence number of this message. */
	seq: number;
	/** Indicates that this commit contained too many ops, or data size was too large. Consumers will need to make a separate request to get missing data. */
	tooBig: boolean;
	/** The repo this event comes from. */
	repo: string;
	/** Repo commit object CID. */
	commit: CID;
	/** The rev of the emitted commit. Note that this information is also in the commit object included in blocks, unless this is a tooBig event. */
	rev: string;
	/** The rev of the last emitted commit from this repo (if any). */
	since: string | null;
	/** CAR file containing relevant blocks, as a diff since the previous repo state. */
	blocks: Uint8Array;
	/** List of repo mutation operations in this commit (eg, records created, updated, or deleted). */
	ops: Array<RepoOp>;
	/** List of new blobs (by CID) referenced by records in this commit. */
	blobs: CID[];
	/** Timestamp of when this message was originally broadcast. */
	time: string;
}
