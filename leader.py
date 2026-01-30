import json
import socket
import threading
import uuid

import config


class LeaderService:
    def __init__(self, host, port=None):
        self.host = host
        self.port = config.LEADER_TCP_PORT if port is None else port
        self.running = False
        self.lock = threading.Lock()
        # worker_id -> {"ip": str, "port": int, "active_auctions": set[str]}
        self.worker_registry = {}
        # auction_id -> worker_id
        self.auction_map = {}
        # auction_id -> {"highest_bid": str, "highest_bidder": str}
        self.auction_status = {}

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen()
        self.port = self.sock.getsockname()[1]

    def start(self):
        """Start the TCP control server for worker registration."""
        print(f"[Leader] LeaderService starting on {self.host}:{self.port}")
        self.running = True
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()
        return accept_thread

    def stop(self):
        """Stop the TCP control server."""
        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass

    def register_worker(self, worker_id, worker_ip, worker_port):
        with self.lock:
            self.worker_registry[worker_id] = {
                "ip": worker_ip,
                "port": int(worker_port),
                "active_auctions": set(),
            }

    def mark_auction_done(self, worker_id, auction_id):
        with self.lock:
            worker = self.worker_registry.get(worker_id)
            if worker:
                worker["active_auctions"].discard(auction_id)
            self.auction_map.pop(auction_id, None)
            self.auction_status.pop(auction_id, None)

    def list_auctions(self):
        with self.lock:
            return {
                auction_id: self._worker_addr(worker_id)
                for auction_id, worker_id in self.auction_map.items()
            }

    def get_worker_for_auction(self, auction_id):
        with self.lock:
            worker_id = self.auction_map.get(auction_id)
            if not worker_id:
                return None
            return self._worker_addr(worker_id)

    def create_auction(self, item, starting_bid, creator_id=None):
        auction_id = str(uuid.uuid4())
        worker = self._allocate_worker(auction_id)
        print(worker)
        if not worker:
            return None

        ok = self._send_worker_create(worker, auction_id, item, starting_bid, creator_id)
        print("ok", ok)

        if not ok:
            self._release_worker(worker["id"], auction_id)
            return None

        with self.lock:
            self.auction_map[auction_id] = worker["id"]
            self.auction_status[auction_id] = {"item": item, "highest_bid": starting_bid}
        print(f"[Leader] Created auction {auction_id} on worker {worker['id']}")
        return auction_id, worker["ip"], worker["port"]

    def handle_udp_request(self, msg_type, parts):
        """Handle client UDP requests and return a response string."""
        if msg_type == config.GET_AUCTIONS_MESSAGE:
            payload = self.list_auctions_with_status()
            return f"{config.AUCTION_LIST_MESSAGE}:{json.dumps(payload)}"
        if msg_type == config.CLIENT_HELLO_MESSAGE:
            payload = self.list_auctions_with_status()
            return f"{config.AUCTION_LIST_MESSAGE}:{json.dumps(payload)}"
        if msg_type == config.CREATE_AUCTION_MESSAGE:
            if len(parts) < 3:
                return "ERROR:BAD_REQUEST"
            item = parts[1]
            price = parts[2]
            creator_id = parts[3] if len(parts) >= 4 else None
            result = self.create_auction(item, price, creator_id)
            if result:
                auction_id, worker_ip, worker_port = result
                return f"SUCCESS:{auction_id}:{worker_ip}:{worker_port}"
            return "ERROR:NO_WORKER"
        if msg_type == config.JOIN_AUCTION_MESSAGE:
            if len(parts) < 2:
                return "ERROR:BAD_REQUEST"
            auction_id = parts[1]
            worker = self.get_worker_for_auction(auction_id)
            if worker:
                worker_ip, worker_port = worker
                return f"WORKER_INFO:{auction_id}:{worker_ip}:{worker_port}"
            return "ERROR:NOT_FOUND"
        return None

    def handle_control_message(self, line):
        """Handle TCP control messages from workers."""
        parts = line.split(":")
        msg_type = parts[0]
        if msg_type == config.WORKER_REGISTER_MESSAGE:
            if len(parts) < 4:
                return "ERROR:BAD_REQUEST"
            worker_id = parts[1]
            worker_ip = parts[2]
            worker_port = parts[3]
            self.register_worker(worker_id, worker_ip, worker_port)
            return "OK"
        if msg_type == config.AUCTION_DONE_MESSAGE:
            if len(parts) < 3:
                return "ERROR:BAD_REQUEST"
            worker_id = parts[1]
            auction_id = parts[2]
            self.mark_auction_done(worker_id, auction_id)
            return "OK"
        if msg_type == config.AUCTION_BID_UPDATE_MESSAGE:
            if len(parts) < 5:
                return "ERROR:BAD_REQUEST"
            auction_id = parts[2]
            highest_bid = parts[3]
            highest_bidder = parts[4]
            with self.lock:
                self.auction_status[auction_id] = {
                    "highest_bid": highest_bid,
                    "highest_bidder": highest_bidder,
                }
            return "OK"
        return "ERROR:UNKNOWN_COMMAND"

    def _allocate_worker(self, auction_id):
        with self.lock:
            candidates = []
            for worker_id, worker in self.worker_registry.items():
                active_count = len(worker["active_auctions"])
                if active_count < config.WORKER_MAX_AUCTIONS:
                    candidates.append((active_count, worker_id, worker))
            if not candidates:
                return None
            candidates.sort()
            _, worker_id, worker = candidates[0]
            worker["active_auctions"].add(auction_id)
            return {
                "id": worker_id,
                "ip": worker["ip"],
                "port": worker["port"],
            }
        return None

    def _release_worker(self, worker_id, auction_id):
        with self.lock:
            worker = self.worker_registry.get(worker_id)
            if worker:
                worker["active_auctions"].discard(auction_id)

    def _send_worker_create(self, worker, auction_id, item, starting_bid, creator_id):
        message_parts = [
            config.AUCTION_CREATE_MESSAGE,
            auction_id,
            item,
            str(starting_bid),
        ]
        if creator_id:
            message_parts.append(creator_id)
        message = ":".join(message_parts)

        try:
            with socket.create_connection((worker["ip"], worker["port"]), timeout=2) as conn:
                conn.sendall((message + "\n").encode())
                with conn.makefile("r") as reader:
                    response = reader.readline().strip()
                    return response.startswith("OK:")
        except OSError:
            return False

    def _worker_addr(self, worker_id):
        worker = self.worker_registry.get(worker_id)
        if not worker:
            return None
        return worker["ip"], worker["port"]

    def list_auctions_with_status(self):
        with self.lock:
            result = {}
            for auction_id, worker_id in self.auction_map.items():
                worker = self.worker_registry.get(worker_id)
                if not worker:
                    continue
                status = self.auction_status.get(auction_id, {})
                result[auction_id] = {
                    "worker_ip": worker["ip"],
                    "worker_port": worker["port"],
                    "highest_bid": status.get("highest_bid"),
                    "highest_bidder": status.get("highest_bidder"),
                    "item": status.get("item")
                }
            return result

    def _accept_loop(self):
        while self.running:
            try:
                conn, addr = self.sock.accept()
            except OSError:
                break
            handler_thread = threading.Thread(
                target=self._handle_control_client,
                args=(conn, addr),
                daemon=True,
            )
            handler_thread.start()

    def _handle_control_client(self, conn, addr):
        with conn, conn.makefile("r") as reader:
            for line in reader:
                line = line.strip()
                if not line:
                    continue
                response = self.handle_control_message(line)
                if response:
                    conn.sendall((response + "\n").encode())
