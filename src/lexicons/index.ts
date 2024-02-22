/**
 * GENERATED CODE - DO NOT MODIFY
 */
import {
  Client as XrpcClient,
  ServiceClient as XrpcServiceClient,
} from '@atproto/xrpc'
import { schemas } from './lexicons'
import { CID } from 'multiformats/cid'
import * as ComAtprotoSyncSubscribeRepos from './types/com/atproto/sync/subscribeRepos'

export * as ComAtprotoSyncSubscribeRepos from './types/com/atproto/sync/subscribeRepos'

export class AtpBaseClient {
  xrpc: XrpcClient = new XrpcClient()

  constructor() {
    this.xrpc.addLexicons(schemas)
  }

  service(serviceUri: string | URL): AtpServiceClient {
    return new AtpServiceClient(this, this.xrpc.service(serviceUri))
  }
}

export class AtpServiceClient {
  _baseClient: AtpBaseClient
  xrpc: XrpcServiceClient
  com: ComNS

  constructor(baseClient: AtpBaseClient, xrpcService: XrpcServiceClient) {
    this._baseClient = baseClient
    this.xrpc = xrpcService
    this.com = new ComNS(this)
  }

  setHeader(key: string, value: string): void {
    this.xrpc.setHeader(key, value)
  }
}

export class ComNS {
  _service: AtpServiceClient
  atproto: AtprotoNS

  constructor(service: AtpServiceClient) {
    this._service = service
    this.atproto = new AtprotoNS(service)
  }
}

export class AtprotoNS {
  _service: AtpServiceClient
  sync: SyncNS

  constructor(service: AtpServiceClient) {
    this._service = service
    this.sync = new SyncNS(service)
  }
}

export class SyncNS {
  _service: AtpServiceClient

  constructor(service: AtpServiceClient) {
    this._service = service
  }
}
