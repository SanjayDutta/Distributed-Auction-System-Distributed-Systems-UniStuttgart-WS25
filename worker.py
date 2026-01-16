import socket
import threading
import time

import config
from auction import Auction


class AuctionWorkerServer:
    def __init__(self, host="0.0.0.0", port=None):
        self.host = host
        self.port = config.WORKER_TCP_PORT if port is None else port
        self.running = True
        # auction_id -> Auction
        self.auctions = {}
        # auction_id -> {client_id: socket}
        self.participant_conns = {}
        # Shared state is guarded because handlers run in threads.
        self.lock = threading.Lock()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen()
        self.port = self.sock.getsockname()[1]

    def start(self):
        # Accept connections in the background so the main thread can do other work.
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()
        return accept_thread

    def stop(self):
        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass

    def _accept_loop(self):
        while self.running:
            try:
                conn, addr = self.sock.accept()
            except OSError:
                break
            handler_thread = threading.Thread(
                target=self._handle_client,
                args=(conn, addr),
                daemon=True,
            )
            handler_thread.start()

    def _handle_client(self, conn, addr):
        # Track auctions joined by this connection so we can clean up on disconnect.
        joined_auctions = {}
        try:
            # Line-based protocol: each request is one line, colon-delimited fields.
            with conn, conn.makefile("r") as reader:
                for line in reader:
                    line = line.strip()
                    if not line:
                        continue
                    response = self._handle_command(line, conn, joined_auctions)
                    if response:
                        self._send_line(conn, response)
        finally:
            self._remove_connection(conn, joined_auctions)

    def _handle_command(self, line, conn, joined_auctions):
        parts = line.split(":")
        msg_type = parts[0]

        if msg_type == config.WORKER_CREATE_MESSAGE:
            return self._handle_create(parts, conn, joined_auctions)
        if msg_type == config.WORKER_JOIN_MESSAGE:
            return self._handle_join(parts, conn, joined_auctions)
        if msg_type == config.WORKER_BID_MESSAGE:
            return self._handle_bid(parts, conn, joined_auctions)
        if msg_type == config.WORKER_STATUS_MESSAGE:
            return self._handle_status(parts)
        return "ERROR:UNKNOWN_COMMAND"

    def _handle_create(self, parts, conn, joined_auctions):
        if len(parts) < 4:
            return "ERROR:BAD_REQUEST"
        auction_id = parts[1]
        item = parts[2]
        starting_bid = parts[3]
        duration_seconds = 60
        creator_id = parts[4] if len(parts) >= 5 else None

        with self.lock:
            if auction_id in self.auctions:
                return "ERROR:ALREADY_EXISTS"
            # Worker owns auction state and schedules its end locally.
            auction = Auction(item, starting_bid, duration_seconds=duration_seconds, auction_id=auction_id)
            self.auctions[auction_id] = auction
            self.participant_conns[auction_id] = {}
            if creator_id:
                auction.add_participant(creator_id)
                self.participant_conns[auction_id][creator_id] = conn
                joined_auctions[auction_id] = creator_id
            self._schedule_close(auction_id, auction.end_time)

        return f"OK:{auction_id}"

    def _handle_join(self, parts, conn, joined_auctions):
        if len(parts) < 3:
            return "ERROR:BAD_REQUEST"
        auction_id = parts[1]
        client_id = parts[2]

        with self.lock:
            auction = self.auctions.get(auction_id)
            if auction is None:
                return "ERROR:NOT_FOUND"
            if not auction.is_open():
                return "ERROR:CLOSED"
            auction.add_participant(client_id)
            self.participant_conns.setdefault(auction_id, {})[client_id] = conn
            joined_auctions[auction_id] = client_id

        return "OK"

    def _handle_bid(self, parts, conn, joined_auctions):
        if len(parts) < 4:
            return "ERROR:BAD_REQUEST"
        auction_id = parts[1]
        client_id = parts[2]
        amount = parts[3]

        with self.lock:
            auction = self.auctions.get(auction_id)
            if auction is None:
                return "ERROR:NOT_FOUND"
            if not auction.is_open():
                return "ERROR:CLOSED"
            ok = auction.place_bid(client_id, amount)
            if ok:
                self.participant_conns.setdefault(auction_id, {})[client_id] = conn
                joined_auctions[auction_id] = client_id
                return f"OK:{auction.highest_bid}"
            return "REJECT:LOW_BID"

    def _handle_status(self, parts):
        if len(parts) < 2:
            return "ERROR:BAD_REQUEST"
        auction_id = parts[1]

        with self.lock:
            auction = self.auctions.get(auction_id)
            if auction is None:
                return "ERROR:NOT_FOUND"
            status = "open" if auction.is_open() else "closed"
            winner = auction.highest_bidder or ""
            return f"STATUS:{status}:{auction.highest_bid}:{winner}"

    def _schedule_close(self, auction_id, end_time):
        if end_time is None:
            return
        # One timer thread per auction to close it at the deadline.
        delay = max(0, end_time - time.time())
        thread = threading.Thread(
            target=self._close_after_delay,
            args=(auction_id, delay),
            daemon=True,
        )
        thread.start()

    def _close_after_delay(self, auction_id, delay):
        time.sleep(delay)
        with self.lock:
            auction = self.auctions.get(auction_id)
            if auction is None or auction.status != "open":
                return
            auction.close()

        # Notify participants outside the lock to avoid blocking new bids/joins.
        self._notify_result(auction_id, auction)

    def _notify_result(self, auction_id, auction):
        with self.lock:
            participants = self.participant_conns.get(auction_id, {}).copy()

        winner = auction.highest_bidder or ""
        result = f"{config.WORKER_RESULT_MESSAGE}:{auction_id}:{winner}:{auction.highest_bid}"

        for client_id, conn in participants.items():
            try:
                self._send_line(conn, result)
            except OSError:
                # Drop dead connections so future notifications don't fail.
                with self.lock:
                    self.participant_conns.get(auction_id, {}).pop(client_id, None)

    def _remove_connection(self, conn, joined_auctions):
        with self.lock:
            for auction_id, client_id in joined_auctions.items():
                conns = self.participant_conns.get(auction_id)
                if conns and conns.get(client_id) is conn:
                    conns.pop(client_id, None)

    def _send_line(self, conn, message):
        conn.sendall((message + "\n").encode())


if __name__ == "__main__":
    server = AuctionWorkerServer()
    print(f"[Worker] TCP server listening on {server.host}:{server.port}")
    server.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()
