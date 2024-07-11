<p align="center">
	<img src="https://github.com/skyware-js/.github/blob/main/assets/logo-dark.png?raw=true" height="72">
</p>
<h1 align="center">@skyware/firehose</h1>

A simple client for consuming data from an AT Protocol Relay.

[Documentation](https://skyware.js.org/docs/firehose)

## Installation

```sh
npm install @skyware/firehose
```

## Example Usage

```js
import { Firehose } from "@skyware/firehose";

const firehose = new Firehose();
firehose.on("commit", (commit) => {
	for (const op of commit.ops) {
		console.log(op);
	}
});
firehose.start();
```

### Events
| Event            | Description                                                                                                                  |
|------------------|------------------------------------------------------------------------------------------------------------------------------|
| `commit`         | Represents a commit to a user's repository.                                                                                  |
| `identity`       | Represents a change to an account's identity. Could be an updated handle, signing key, or PDS hosting endpoint.              |
| `handle`         | Represents an update of an account's handle, or transition to/from invalid state (may be deprecated in favor of `identity`). |
| `tombstone`      | Indicates that an account has been deleted (may be deprecated in favor of `identity` or a future `account` event).           |
| `info`           | An informational message from the relay.                                                                                     |
| `open`           | Emitted when the websocket connection is opened.                                                                             |
| `close`          | Emitted when the websocket connection is closed.                                                                             |
| `error`          | Emitted when an error occurs while handling a message.                                                                       |
| `websocketError` | Emitted when an error occurs with the websocket connection.                                                                  |

