/**
 * GENERATED CODE - DO NOT MODIFY
 */
import { LexiconDoc, Lexicons } from '@atproto/lexicon'

export const schemaDict = {
  ComAtprotoSyncSubscribeRepos: {
    lexicon: 1,
    id: 'com.atproto.sync.subscribeRepos',
    defs: {
      main: {
        type: 'subscription',
        description:
          'Repository event stream, aka Firehose endpoint. Outputs repo commits with diff data, and identity update events, for all repositories on the current server. See the atproto specifications for details around stream sequencing, repo versioning, CAR diff format, and more. Public and does not require auth; implemented by PDS and Relay.',
        parameters: {
          type: 'params',
          properties: {
            cursor: {
              type: 'integer',
              description: 'The last known event seq number to backfill from.',
            },
          },
        },
        message: {
          schema: {
            type: 'union',
            refs: [
              'lex:com.atproto.sync.subscribeRepos#commit',
              'lex:com.atproto.sync.subscribeRepos#identity',
              'lex:com.atproto.sync.subscribeRepos#handle',
              'lex:com.atproto.sync.subscribeRepos#migrate',
              'lex:com.atproto.sync.subscribeRepos#tombstone',
              'lex:com.atproto.sync.subscribeRepos#info',
            ],
          },
        },
        errors: [
          {
            name: 'FutureCursor',
          },
          {
            name: 'ConsumerTooSlow',
            description:
              'If the consumer of the stream can not keep up with events, and a backlog gets too large, the server will drop the connection.',
          },
        ],
      },
      commit: {
        type: 'object',
        description:
          'Represents an update of repository state. Note that empty commits are allowed, which include no repo data changes, but an update to rev and signature.',
        required: [
          'seq',
          'rebase',
          'tooBig',
          'repo',
          'commit',
          'rev',
          'since',
          'blocks',
          'ops',
          'blobs',
          'time',
        ],
        nullable: ['prev', 'since'],
        properties: {
          seq: {
            type: 'integer',
            description: 'The stream sequence number of this message.',
          },
          rebase: {
            type: 'boolean',
            description: 'DEPRECATED -- unused',
          },
          tooBig: {
            type: 'boolean',
            description:
              'Indicates that this commit contained too many ops, or data size was too large. Consumers will need to make a separate request to get missing data.',
          },
          repo: {
            type: 'string',
            format: 'did',
            description: 'The repo this event comes from.',
          },
          commit: {
            type: 'cid-link',
            description: 'Repo commit object CID.',
          },
          prev: {
            type: 'cid-link',
            description:
              'DEPRECATED -- unused. WARNING -- nullable and optional; stick with optional to ensure golang interoperability.',
          },
          rev: {
            type: 'string',
            description:
              'The rev of the emitted commit. Note that this information is also in the commit object included in blocks, unless this is a tooBig event.',
          },
          since: {
            type: 'string',
            description:
              'The rev of the last emitted commit from this repo (if any).',
          },
          blocks: {
            type: 'bytes',
            description:
              'CAR file containing relevant blocks, as a diff since the previous repo state.',
            maxLength: 1000000,
          },
          ops: {
            type: 'array',
            items: {
              type: 'ref',
              ref: 'lex:com.atproto.sync.subscribeRepos#repoOp',
              description:
                'List of repo mutation operations in this commit (eg, records created, updated, or deleted).',
            },
            maxLength: 200,
          },
          blobs: {
            type: 'array',
            items: {
              type: 'cid-link',
              description:
                'List of new blobs (by CID) referenced by records in this commit.',
            },
          },
          time: {
            type: 'string',
            format: 'datetime',
            description:
              'Timestamp of when this message was originally broadcast.',
          },
        },
      },
      identity: {
        type: 'object',
        description:
          "Represents a change to an account's identity. Could be an updated handle, signing key, or pds hosting endpoint. Serves as a prod to all downstream services to refresh their identity cache.",
        required: ['seq', 'did', 'time'],
        properties: {
          seq: {
            type: 'integer',
          },
          did: {
            type: 'string',
            format: 'did',
          },
          time: {
            type: 'string',
            format: 'datetime',
          },
        },
      },
      handle: {
        type: 'object',
        description:
          "Represents an update of the account's handle, or transition to/from invalid state. NOTE: Will be deprecated in favor of #identity.",
        required: ['seq', 'did', 'handle', 'time'],
        properties: {
          seq: {
            type: 'integer',
          },
          did: {
            type: 'string',
            format: 'did',
          },
          handle: {
            type: 'string',
            format: 'handle',
          },
          time: {
            type: 'string',
            format: 'datetime',
          },
        },
      },
      migrate: {
        type: 'object',
        description:
          'Represents an account moving from one PDS instance to another. NOTE: not implemented; account migration uses #identity instead',
        required: ['seq', 'did', 'migrateTo', 'time'],
        nullable: ['migrateTo'],
        properties: {
          seq: {
            type: 'integer',
          },
          did: {
            type: 'string',
            format: 'did',
          },
          migrateTo: {
            type: 'string',
          },
          time: {
            type: 'string',
            format: 'datetime',
          },
        },
      },
      tombstone: {
        type: 'object',
        description:
          'Indicates that an account has been deleted. NOTE: may be deprecated in favor of #identity or a future #account event',
        required: ['seq', 'did', 'time'],
        properties: {
          seq: {
            type: 'integer',
          },
          did: {
            type: 'string',
            format: 'did',
          },
          time: {
            type: 'string',
            format: 'datetime',
          },
        },
      },
      info: {
        type: 'object',
        required: ['name'],
        properties: {
          name: {
            type: 'string',
            knownValues: ['OutdatedCursor'],
          },
          message: {
            type: 'string',
          },
        },
      },
      repoOp: {
        type: 'object',
        description: 'A repo operation, ie a mutation of a single record.',
        required: ['action', 'path', 'cid'],
        nullable: ['cid'],
        properties: {
          action: {
            type: 'string',
            knownValues: ['create', 'update', 'delete'],
          },
          path: {
            type: 'string',
          },
          cid: {
            type: 'cid-link',
            description:
              'For creates and updates, the new record CID. For deletions, null.',
          },
        },
      },
    },
  },
}
export const schemas: LexiconDoc[] = Object.values(schemaDict) as LexiconDoc[]
export const lexicons: Lexicons = new Lexicons(schemas)
export const ids = {
  ComAtprotoSyncSubscribeRepos: 'com.atproto.sync.subscribeRepos',
}
