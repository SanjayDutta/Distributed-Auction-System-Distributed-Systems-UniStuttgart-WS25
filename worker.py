import csv
import os
import socket
import threading
import time
import uuid

import config
from auction import Auction


class WorkerService:
    def __init__(self, worker_id, worker_ip, host="0.0.0.0", port=None, leader_port=None):
        """Worker role: auction TCP server + leader control client."""
        self.worker_id = worker_id
        self.worker_ip = worker_ip
        self.leader_ip = None
        self.leader_port = config.LEADER_TCP_PORT if leader_port is None else leader_port

        self.host = host
        self.port = 0 if port is None else port
        self.running = True
        self.started = False
        # auction_id -> Auction
        self.auctions = {}
        # auction_id -> {client_id: socket}
        self.participant_conns = {}
        # Shared state is guarded because handlers run in threads.
        self.lock = threading.Lock()
        
        # CSV persistence file
        self.csv_file = f"auctions_{worker_id}.csv"
        self._init_csv_file()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen()
        self.port = self.sock.getsockname()[1]

    def start(self, leader_ip=None):
        """Start accepting TCP connections and optionally register with leader."""
        if self.started:
            return None
        print(f"[Worker] WorkerService starting on {self.host}:{self.port}")
        self.started = True
        # Accept connections in the background so the main thread can do other work.
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()
        if leader_ip:
            self.leader_ip = leader_ip
            self._register_with_leader()
        return accept_thread

    def stop(self):
        """Stop the server and close the listening socket."""
        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass

    def _accept_loop(self):
        """Accept incoming TCP connections and spawn a handler thread per client."""
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
        """Handle a single client connection using a line-based request protocol."""
        # Track auctions joined by this connection so we can clean up on disconnect.
        joined_auctions = {}
        # print("hello????????????????????????? handle_client")
        # while True:
                
            # try:
            #     print("type", type(conn), conn)
            #     data, addr = conn.recvfrom(1024)
            #     print("GOT", data.decode().strip()) 
            #     response = self._handle_command(data.decode().strip(), conn, joined_auctions)
            #     if response:
            #         self._send_line(conn, response)
    
                
            # except Exception as e:
            #     print("Failed to handle client", e)
            #     return
        try:     
            # Line-based protocol: each request is one line, colon-delimited fields.
            with conn, conn.makefile("r") as reader:
                for line in reader:
                    # print("line", line)
                    line = line.strip()
                    if not line:
                        # print("skipping")
                        continue
                    response = self._handle_command(line, conn, joined_auctions)
                    if response:
                        self._send_line(conn, response)
        finally:
            self._cleanup_disconnected_client(conn, joined_auctions)


    def _handle_command(self, line, conn, joined_auctions):
        """Parse a single request line and dispatch to the correct handler."""
        parts = line.split(":")
        msg_type = parts[0]

        # print("HANDLE COMMAND", msg_type)

        if msg_type == config.AUCTION_CREATE_MESSAGE:
            return self._handle_create(parts, conn, joined_auctions)
        if msg_type == config.AUCTION_JOIN_MESSAGE:
            return self._handle_join(parts, conn, joined_auctions)
        if msg_type == config.AUCTION_BID_MESSAGE:
            return self._handle_bid(parts, conn, joined_auctions)
        if msg_type == config.AUCTION_STATUS_MESSAGE:
            return self._handle_status(parts)
        if msg_type == "AUCTION_RECOVER":
            return self._handle_recover(parts)
        if msg_type == config.HEARTBEAT_MESSAGE:
            return "ALIVE"
        
        print("RETURNING ERROR")
        return "ERROR:UNKNOWN_COMMAND"

    def _handle_create(self, parts, conn, joined_auctions):
        """Create a new auction and optionally auto-join the creator."""
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
            
            # Persist to CSV
            self._persist_auction_to_csv(auction_id, auction)

        return f"OK:{auction_id}"

    def _handle_join(self, parts, conn, joined_auctions):
        """Register a client as a participant for a given auction."""
        if len(parts) < 3:
            return "ERROR:BAD_REQUEST"
        auction_id = parts[1]
        client_id = parts[2]
        
        # print("???????????????? handle join", parts)

        with self.lock:
            auction = self.auctions.get(auction_id)
            if auction is None:
                return "ERROR:NOT_FOUND"
            if not auction.is_open():
                return "ERROR:CLOSED"
            auction.add_participant(client_id)
            self.participant_conns.setdefault(auction_id, {})[client_id] = conn
            joined_auctions[auction_id] = client_id

        auction_sequence_number = auction.auction_sequence_number
        return f"OK:{auction_sequence_number}"

    def _handle_bid(self, parts, conn, joined_auctions):
        """Place a bid on an auction and update highest bid if valid."""
        if len(parts) < 5:
            return "ERROR:BAD_REQUEST"
        auction_id = parts[1]
        client_id = parts[2]
        amount = parts[3]
        auction_sequence_number = int(parts[4])

        response = None
        broadcast = None
        with self.lock:
            auction = self.auctions.get(auction_id)
            if auction is None:
                return "ERROR:NOT_FOUND"
            if not auction.is_open():
                return "ERROR:CLOSED"
            if auction_sequence_number <= auction.auction_sequence_number:
                return "ERROR:MSG_OUT_OF_ORDER"
            
            ok = auction.place_bid(client_id, amount)
            auction.auction_sequence_number += 1
            if ok:
                self.participant_conns.setdefault(auction_id, {})[client_id] = conn
                joined_auctions[auction_id] = client_id
                response = f"OK:{auction.highest_bid}:{auction.auction_sequence_number}"
                broadcast = (auction_id, auction.highest_bid, auction.highest_bidder, auction.auction_sequence_number)
                # Persist bid update to CSV
                self._persist_auction_to_csv(auction_id, auction)
            else:
                response = f"REJECT:LOW_BID:{auction.auction_sequence_number}"
        # Broadcast highest bid update to participants.
        if broadcast:
            self._broadcast_bid_update(*broadcast)
            self.notify_bid_update(*broadcast)
        return response

    def _handle_status(self, parts):
        """Return current status of an auction (open/closed, highest bid, winner)."""
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

    def _handle_recover(self, parts):
        """Recover a previously lost auction from leader (after worker failure and recovery)."""
        if len(parts) < 8:
            return "ERROR:BAD_REQUEST"
        
        auction_id = parts[1]
        item = parts[2]
        starting_bid = parts[3]
        highest_bid = parts[4]
        highest_bidder = parts[5] if parts[5] else None
        status = parts[6]
        end_time_str = parts[7]
        
        try:
            end_time = float(end_time_str) if end_time_str else None
        except ValueError:
            end_time = None
        
        with self.lock:
            if auction_id in self.auctions:
                return "ERROR:ALREADY_EXISTS"
            
            # Recreate auction from recovered data
            auction = Auction(item, starting_bid, duration_seconds=None, auction_id=auction_id, start_time=None)
            auction.highest_bid = float(highest_bid)
            auction.highest_bidder = highest_bidder
            auction.status = status
            auction.end_time = end_time
            
            self.auctions[auction_id] = auction
            self.participant_conns[auction_id] = {}
            
            # If auction is already closed, don't schedule a close timer
            if status != "closed" and end_time:
                self._schedule_close(auction_id, end_time)
            
            # Persist to CSV
            self._persist_auction_to_csv(auction_id, auction)
        
        print(f"[Worker] Recovered auction {auction_id} (item: {item}, status: {status})")
        return f"OK:{auction_id}"

    def _schedule_close(self, auction_id, end_time):
        """Start a timer thread to close the auction at its deadline."""
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
        """Sleep until the deadline, then close the auction and notify participants."""
        time.sleep(delay)
        with self.lock:
            auction = self.auctions.get(auction_id)
            if auction is None or auction.status != "open":
                return
            auction.close()
            # Persist closed status to CSV
            self._persist_auction_to_csv(auction_id, auction)

        # Notify participants outside the lock to avoid blocking new bids/joins.
        self._notify_result(auction_id, auction)
        self.notify_auction_done(auction_id)

    def _notify_result(self, auction_id, auction):
        """Send final result to all participants still connected to this auction."""
        with self.lock:
            participants = self.participant_conns.get(auction_id, {}).copy()

        winner = auction.highest_bidder or ""
        result = f"{config.AUCTION_RESULT_MESSAGE}:{auction_id}:{winner}:{auction.highest_bid}"

        for client_id, conn in participants.items():
            try:
                self._send_line(conn, result)
            except OSError:
                # Drop dead connections so future notifications don't fail.
                with self.lock:
                    self.participant_conns.get(auction_id, {}).pop(client_id, None)

    def _cleanup_disconnected_client(self, conn, joined_auctions):
        """Remove a disconnected client's socket from all auction participant maps."""
        with self.lock:
            for auction_id, client_id in joined_auctions.items():
                conns = self.participant_conns.get(auction_id)
                if conns and conns.get(client_id) is conn:
                    conns.pop(client_id, None) # remove C1 -> conn1 from participant_conns A1

    def _broadcast_bid_update(self, auction_id, highest_bid, highest_bidder, auction_sequence_number):
        """Broadcast highest bid updates to all participants of the auction."""
        with self.lock:
            participants = self.participant_conns.get(auction_id, {}).copy()

        bidder = highest_bidder or ""
        message = f"{config.AUCTION_BID_UPDATE_MESSAGE}:{auction_id}:{highest_bid}:{bidder}:{auction_sequence_number}"

        for client_id, conn in participants.items():
            try:
                self._send_line(conn, message)
            except OSError:
                with self.lock:
                    self.participant_conns.get(auction_id, {}).pop(client_id, None)

    def _send_line(self, conn, message):
        """Send a single line response to the client."""
        conn.sendall((message + "\n").encode())

    def _register_with_leader(self):
        if not self.leader_ip or self.port is None:
            return
        message = f"{config.WORKER_REGISTER_MESSAGE}:{self.worker_id}:{self.worker_ip}:{self.port}"
        self._send_control_message(message)

    def notify_auction_done(self, auction_id):
        message = f"{config.AUCTION_DONE_MESSAGE}:{self.worker_id}:{auction_id}"
        self._send_control_message(message)

    def notify_bid_update(self, auction_id, highest_bid, highest_bidder, auction_sequence_number):
        bidder = highest_bidder or ""
        message = (
            f"{config.AUCTION_BID_UPDATE_MESSAGE}:{self.worker_id}:"
            f"{auction_id}:{highest_bid}:{bidder}:{auction_sequence_number}"
        )
        self._send_control_message(message)

    def _send_control_message(self, message):
        try:
            with socket.create_connection((self.leader_ip, self.leader_port), timeout=2) as conn:
                conn.sendall((message + "\n").encode())
                with conn.makefile("r") as reader:
                    reader.readline()
        except OSError:
            return

    def _init_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['auction_id', 'item', 'starting_bid', 'highest_bid', 'highest_bidder', 'status', 'participants', 'end_time'])

    def _persist_auction_to_csv(self, auction_id, auction):
        """Write or update auction data to CSV file."""
        try:
            # Read all rows
            rows = []
            if os.path.exists(self.csv_file):
                with open(self.csv_file, 'r', newline='') as f:
                    reader = csv.reader(f)
                    rows = list(reader)
            
            # Find and update or append
            header = rows[0] if rows else ['auction_id', 'item', 'starting_bid', 'highest_bid', 'highest_bidder', 'status', 'participants', 'end_time']
            data_rows = rows[1:] if len(rows) > 1 else []
            
            # Check if auction exists in CSV
            auction_found = False
            for i, row in enumerate(data_rows):
                if row and row[0] == auction_id:
                    # Update existing row
                    data_rows[i] = [
                        auction_id,
                        auction.item,
                        str(auction.starting_bid),
                        str(auction.highest_bid),
                        auction.highest_bidder or '',
                        auction.status,
                        ','.join(auction.participants),
                        str(auction.end_time) if auction.end_time else ''
                    ]
                    auction_found = True
                    break
            
            if not auction_found:
                # Add new row
                data_rows.append([
                    auction_id,
                    auction.item,
                    str(auction.starting_bid),
                    str(auction.highest_bid),
                    auction.highest_bidder or '',
                    auction.status,
                    ','.join(auction.participants),
                    str(auction.end_time) if auction.end_time else ''
                ])
            
            # Write back to file
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(data_rows)
        except Exception as e:
            print(f"[Worker] Failed to persist auction to CSV: {e}")


if __name__ == "__main__":
    worker_id = str(uuid.uuid4())
    worker_ip = config.get_network_ip()
    server = WorkerService(worker_id, worker_ip)
    print(f"[Worker] TCP server listening on {server.host}:{server.port}")
    server.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()
