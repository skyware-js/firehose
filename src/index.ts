import { Frame } from "@atproto/xrpc-server";
import { EventEmitter } from "node:events";
import WS, { WebSocket } from "ws";
import * as ComAtprotoSyncSubscribeRepos from "./lexicons/types/com/atproto/sync/subscribeRepos";

interface FirehoseOptions {
	/**
	 * The cursor to listen from. If not provided, the firehose will start from the latest event.
	 */
	cursor?: string;
	/**
	 * How frequently to update the stored cursor, in milliseconds.
	 * @default 5000
	 */
	setCursorInterval?: number;
}

export class Firehose extends EventEmitter {
	ws?: WebSocket;

	cursor = "";

	private cursorInterval?: NodeJS.Timeout | undefined;

	constructor(public relay = "wss://bsky.network", private options: FirehoseOptions = {}) {
		super();
		this.cursor = options.cursor ?? "";
		this.options.setCursorInterval ??= 5000;
	}

	start() {
		this.ws = new WebSocket(`${this.relay}/xrpc/com.atproto.sync.subscribeRepos${this.cursor}`);

		this.ws.on("open", () => {
			this.emit("open");
		});

		this.ws.on("message", (data) => {
			try {
				const message = this.parseMessage(data);
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
	}

	close() {
		this.ws?.close();
	}

	/** Emitted when the connection is opened. */
	override on(event: "open", listener: () => void): this;
	/** Emitted when the connection is closed. */
	override on(event: "close", listener: (cursor: string) => void): this;
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
	override on(
		event: "commit",
		listener: (
			message: ComAtprotoSyncSubscribeRepos.Commit & {
				$type: "com.atproto.sync.subscribeRepos#commit";
			},
		) => void,
	): this;
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
		return super.on(event, listener);
	}

	private parseMessage(data: WS.RawData) {
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

		return { $type: `com.atproto.sync.subscribeRepos${frame.header.t}`, ...frame.body };
	}

	/** Sets the cursor once every `setCursorInterval` milliseconds. */
	private setCursor(cursor: string) {
		if (this.cursorInterval) return;
		this.cursorInterval = setTimeout(() => {
			this.cursor = cursor;
			this.cursorInterval = undefined;
		}, this.options.setCursorInterval);
	}
}
