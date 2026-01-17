# Distributed Auction System - Current Status

## Implemented
- **Node role selection**: Nodes run discovery + HS election and then take a role.
  - Leader: starts `LeaderService` (TCP control server + auction assignment logic).
  - Worker: starts `WorkerService` (auction TCP server + leader control client).
- **Leader responsibilities** (`leader.py`):
  - Worker registry with capacity-aware allocation (`WORKER_MAX_AUCTIONS`).
  - Mapping `auction_id -> worker` and active auction tracking per worker.
  - TCP control channel for `WORKER_REGISTER` and `AUCTION_DONE` messages.
  - UDP handling for `CLIENT_HELLO` (returns auction list), `CREATE_AUCTION`, `JOIN_AUCTION`, `GET_AUCTIONS`.
  - `AUCTION_BID_UPDATE` tracking for latest highest bid/ bidder.
- **Worker responsibilities** (`worker.py`):
  - TCP auction server with `AUCTION_CREATE`, `AUCTION_JOIN`, `AUCTION_BID`, `AUCTION_STATUS`.
  - Broadcasts `AUCTION_BID_UPDATE` to all participants.
  - Sends `AUCTION_RESULT` at auction end (60s fixed duration).
  - Reports `AUCTION_BID_UPDATE` + `AUCTION_DONE` to leader via TCP control channel.
- **Auction model** (`auction.py`):
  - Tracks id, item, starting bid, highest bid/bidder, status, end time, participants.
- **Test client** (`test.py`):
  - CLI commands: `hello`, `create`, `bid`, `status`.
  - Two-client workflow supported (creator listens; bidder bids higher and listens).

## Not Implemented / Pending Integration
- **Real client integration** (`client.py`):
  - No TCP worker interaction yet (JOIN/BID/LISTEN).
  - No handling of `AUCTION_*` messages or continuous updates.
- **Leader-client auction list format**:
  - `CLIENT_HELLO` returns JSON list but `client.py` does not parse it yet.
- **Robustness / retries**:
  - UDP messages have no ACK/retry (except test client retries).
  - No worker heartbeat or re-registration after leader change.
- **State recovery**:
  - Target design: leader acts as the metadata center; workers manage their own auctions.
  - On leader failure, a new leader should request workers to re-report their auction lists (snapshot recovery).
  - Not implemented yet: worker persistence or re-sync logic.
