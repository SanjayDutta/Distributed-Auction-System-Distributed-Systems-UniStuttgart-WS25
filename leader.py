import csv
import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import config


class LeaderHTTPHandler(BaseHTTPRequestHandler):
    """HTTP request handler for client-leader communication."""
    leader_service = None  # Set by LeaderService
    
    def log_message(self, format, *args):
        """Suppress default HTTP logging."""
        pass
    
    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/auctions':
            # List all auctions
            try:
                auctions = self.leader_service.list_auctions_with_status()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(auctions).encode())
            except Exception as e:
                self.send_error(500, f"Internal error: {e}")
        
        elif parsed_path.path.startswith('/auctions/'):
            # Get worker info for specific auction
            auction_id = parsed_path.path.split('/')[-1]
            try:
                worker_info = self.leader_service.get_worker_for_auction(auction_id)
                if worker_info:
                    result = {
                        "worker_ip": worker_info["ip"],
                        "worker_port": worker_info["port"]
                    }
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(result).encode())
                else:
                    self.send_error(404, "Auction not found")
            except Exception as e:
                self.send_error(500, f"Internal error: {e}")
        else:
            self.send_error(404, "Not found")
    
    def do_POST(self):
        """Handle POST requests."""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/auctions':
            # Create new auction
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode())
                
                item = data.get('item')
                starting_bid = data.get('starting_bid')
                
                if not item or not starting_bid:
                    self.send_error(400, "Missing item or starting_bid")
                    return
                
                result = self.leader_service.create_auction(item, starting_bid)
                if result:
                    auction_id, worker_ip, worker_port = result
                    response = {
                        "auction_id": auction_id,
                        "worker_ip": worker_ip,
                        "worker_port": worker_port
                    }
                    self.send_response(201)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(response).encode())
                else:
                    self.send_error(503, "No workers available")
            except Exception as e:
                self.send_error(500, f"Internal error: {e}")
        else:
            self.send_error(404, "Not found")


class LeaderService:
    def __init__(self, host, port=None, recover_from_worker_id=None):
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
        
        # Store worker_id for auction recovery (when worker becomes leader)
        self.recover_from_worker_id = recover_from_worker_id

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen()
        self.port = self.sock.getsockname()[1]

        # HTTP server for client communication
        self.http_server = None
        self.http_thread = None

        self.heartbeat_worker_thread = threading.Thread(target=self.heartbeat_worker_server, daemon=True)
        self.heartbeat_worker_thread.start()

    def start(self):
        """Start the TCP control server for worker registration and HTTP server for clients."""
        print(f"[Leader] LeaderService starting on {self.host}:{self.port}")
        self.running = True
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()
        
        # Start HTTP server for client communication
        try:
            self.http_server = HTTPServer((self.host, config.LEADER_HTTP_PORT), LeaderHTTPHandler)
            LeaderHTTPHandler.leader_service = self
            self.http_thread = threading.Thread(target=self.http_server.serve_forever, daemon=True)
            self.http_thread.start()
            print(f"[Leader] HTTP server started on {self.host}:{config.LEADER_HTTP_PORT}")
        except Exception as e:
            print(f"[Leader] Failed to start HTTP server: {e}")
        
        # If we have auctions to recover (worker became leader), trigger recovery
        if self.recover_from_worker_id:
            print(f"[Leader] Detected auction data from worker {self.recover_from_worker_id[:8]}, triggering recovery...")
            recovery_thread = threading.Thread(target=self.trigger_auction_recovery, daemon=True)
            recovery_thread.start()
        
        return accept_thread

    def stop(self):
        """Stop the TCP control server and HTTP server."""
        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass
        
        # Stop HTTP server
        if self.http_server:
            try:
                self.http_server.shutdown()
                print("[Leader] HTTP server stopped")
            except Exception as e:
                print(f"[Leader] Error stopping HTTP server: {e}")

    def register_worker(self, worker_id, worker_ip, worker_port, auction_inventory=None):
        with self.lock:
            self.worker_registry[worker_id] = {
                "ip": worker_ip,
                "port": int(worker_port),
                "active_auctions": set(),
            }
            
            # Rebuild auction_map and auction_status from worker's inventory
            if auction_inventory:
                print(f"[Leader] Rebuilding auction state from worker {worker_id[:8]} - {len(auction_inventory)} auctions")
                for auction_data in auction_inventory:
                    auction_id = auction_data.get("auction_id")
                    if auction_id:
                        # Add to auction_map
                        self.auction_map[auction_id] = worker_id
                        self.worker_registry[worker_id]["active_auctions"].add(auction_id)
                        
                        # Rebuild auction_status
                        self.auction_status[auction_id] = {
                            "item": auction_data.get("item"),
                            "highest_bid": auction_data.get("highest_bid"),
                            "highest_bidder": auction_data.get("highest_bidder"),
                        }
                        print(f"[Leader] Restored auction {auction_id[:8]} on worker {worker_id[:8]}")

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
            
            # Parse auction inventory if provided (for leader failover)
            auction_inventory = []
            if len(parts) >= 5:
                try:
                    auction_json = ":".join(parts[4:])  # Rejoin in case JSON has colons
                    auction_inventory = json.loads(auction_json)
                except Exception as e:
                    print(f"[Leader] Failed to parse auction inventory: {e}")
            
            self.register_worker(worker_id, worker_ip, worker_port, auction_inventory)
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
        if msg_type == config.HEARTBEAT_MESSAGE:
            return "ALIVE"

        return "ERROR:UNKNOWN_COMMAND"

    def _allocate_worker(self, auction_id, retry_spawn=True):
        with self.lock:
            candidates = []
            for worker_id, worker in self.worker_registry.items():
                active_count = len(worker["active_auctions"])
                if active_count < config.WORKER_MAX_AUCTIONS:
                    candidates.append((active_count, worker_id, worker))
            if not candidates:
                if retry_spawn:
                    # Release lock before spawning to avoid blocking
                    pass
                else:
                    return None
            else:
                candidates.sort()
                _, worker_id, worker = candidates[0]
                worker["active_auctions"].add(auction_id)
                return {
                    "id": worker_id,
                    "ip": worker["ip"],
                    "port": worker["port"],
                }
        
        # If we reach here and retry_spawn is True, try spawning a new worker
        if retry_spawn:
            self._spawn_worker_node()
            time.sleep(2)  # Wait for new worker to register
            return self._allocate_worker(auction_id, retry_spawn=False)
        
        return None

    def _spawn_worker_node(self):
        """Spawn a new node.py process in a separate Terminal window (OS-specific)."""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            node_script = os.path.join(script_dir, 'node.py')
            
            system = platform.system()
            
            if system == 'Darwin':  # macOS
                cmd = f"cd '{script_dir}' && python node.py"
                apple_script = f'tell application "Terminal" to do script "{cmd}"'
                subprocess.Popen(['osascript', '-e', apple_script])
                
            elif system == 'Windows':
                subprocess.Popen(f'start cmd /k "cd /d {script_dir} && python node.py"', shell=True)
                
            else:  # Linux and other Unix-like systems
                # Try common terminal emulators in order of preference
                terminal_cmds = [
                    ['gnome-terminal', '--', 'bash', '-c', f'cd {script_dir} && python node.py; bash'],
                    ['xterm', '-hold', '-e', f'bash -c "cd {script_dir} && python node.py"'],
                    ['konsole', '--noclose', '-e', 'bash', '-c', f'cd {script_dir} && python node.py'],
                ]
                
                for cmd in terminal_cmds:
                    try:
                        subprocess.Popen(cmd)
                        break
                    except FileNotFoundError:
                        continue
            
            print("[Leader] Spawned new worker node in a separate Terminal")
        except Exception as e:
            print(f"[Leader] Failed to spawn worker node: {e}")

    def _recover_worker_auctions(self, worker_id):
        """Read auction data from worker's CSV file and return as list of dicts."""
        csv_file = f"auctions_{worker_id}.csv"
        auctions = []
        
        if not os.path.exists(csv_file):
            return auctions
        
        try:
            with open(csv_file, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row and row.get('auction_id'):
                        auctions.append(row)
            print(f"[Leader] Recovered {len(auctions)} auctions from worker {worker_id}")
        except Exception as e:
            print(f"[Leader] Failed to recover auctions from {csv_file}: {e}")
        
        return auctions

    def trigger_auction_recovery(self):
        """Unified recovery: spawn worker and reassign auctions from previous worker (now leader)."""
        if not self.recover_from_worker_id:
            return
        
        print(f"[Leader] Starting unified auction recovery for worker {self.recover_from_worker_id[:8]}")
        self._reassign_auctions_with_recovery(self.recover_from_worker_id)

    def _reassign_auctions_with_recovery(self, dead_worker_id):
        """When a worker fails, spawn a fresh worker and recover auctions to it."""
        print(f"[Leader] Attempting to recover auctions from worker {dead_worker_id[:8]}")
        
        recovered_auctions = self._recover_worker_auctions(dead_worker_id)
        
        if not recovered_auctions:
            print(f"[Leader] No auctions to recover from worker {dead_worker_id}")
            return
        
        # Always spawn a fresh worker for recovery (more reliable than checking existing workers)
        print(f"[Leader] Spawning fresh worker for auction recovery...")
        self._spawn_worker_node()
        
        # Wait for new worker to register (spawn + discovery + registration takes ~5-6 seconds)
        print(f"[Leader] Waiting 6 seconds for new worker to register...")
        time.sleep(6)
        
        # Try to recover each auction
        for auction_data in recovered_auctions:
            auction_id = auction_data.get('auction_id')
            if not auction_id:
                continue
            
            # Only reassign if not already handled
            if auction_id not in self.auction_map:
                # Find the most recently registered worker (the one we just spawned)
                new_worker = self._get_newest_worker()
                
                if new_worker:
                    # Send recovered auction data to new worker for restoration
                    success = self._send_recovered_auction_to_worker(new_worker, auction_data)
                    
                    if success:
                        with self.lock:
                            self.auction_map[auction_id] = new_worker['id']
                        print(f"[Leader] Reassigned auction {auction_id} to worker {new_worker['id'][:8]} at {new_worker['ip']}:{new_worker['port']}")
                    else:
                        print(f"[Leader] Failed to recover auction {auction_id} on new worker")
                else:
                    print(f"[Leader] No new worker available for recovery (spawn may have failed)")

    def _get_newest_worker(self):
        """Get the most recently registered worker (lowest active auction count)."""
        with self.lock:
            if not self.worker_registry:
                return None
            
            # Find worker with fewest active auctions (newly spawned will have 0)
            candidates = []
            for worker_id, worker in self.worker_registry.items():
                active_count = len(worker["active_auctions"])
                candidates.append((active_count, worker_id, worker))
            
            if not candidates:
                return None
            
            candidates.sort()
            _, worker_id, worker = candidates[0]
            
            return {
                "id": worker_id,
                "ip": worker["ip"],
                "port": worker["port"],
            }

    def _send_recovered_auction_to_worker(self, worker, auction_data):
        """Send recovered auction data to a worker so it can restore the auction."""
        try:
            # First, verify worker is alive with a heartbeat
            try:
                with socket.create_connection((worker["ip"], worker["port"]), timeout=2) as conn:
                    conn.sendall((config.HEARTBEAT_MESSAGE + "\n").encode())
                    with conn.makefile("r") as reader:
                        response = reader.readline().strip()
                        if response != "ALIVE":
                            print(f"[Leader] Worker {worker['id']} not responding to heartbeat, skipping recovery")
                            return False
            except Exception as e:
                print(f"[Leader] Worker {worker['id']} health check failed: {e}")
                return False
            
            auction_id = auction_data.get('auction_id')
            item = auction_data.get('item', '')
            starting_bid = auction_data.get('starting_bid', '0')
            highest_bid = auction_data.get('highest_bid', starting_bid)
            highest_bidder = auction_data.get('highest_bidder', '')
            status = auction_data.get('status', 'open')
            end_time = auction_data.get('end_time', '')
            
            # Send AUCTION_RECOVER message to worker
            # Format: AUCTION_RECOVER:auction_id:item:starting_bid:highest_bid:highest_bidder:status:end_time
            message = ":".join([
                "AUCTION_RECOVER",
                auction_id,
                item,
                str(starting_bid),
                str(highest_bid),
                highest_bidder,
                status,
                str(end_time)
            ])
            
            with socket.create_connection((worker["ip"], worker["port"]), timeout=2) as conn:
                conn.sendall((message + "\n").encode())
                with conn.makefile("r") as reader:
                    response = reader.readline().strip()
                    if response.startswith("OK"):
                        print(f"[Leader] Successfully sent auction {auction_id} to worker {worker['id']}")
                        return True
                    else:
                        print(f"[Leader] Worker {worker['id']} failed to recover auction: {response}")
                        return False
        except Exception as e:
            print(f"[Leader] Failed to send recovered auction to worker: {e}")
            return False


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
        print(message)

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

    def heartbeat_worker_server(self):
        """
        Heartbeat every 5 seconds to workers.
        If a worker does not respond 3x, re assign auctions to other workers
        """
        
        failed_heartbeats = {id: 0 for id in self.worker_registry.keys()}

        while True:
            time.sleep(5)
            for id in list(self.worker_registry.keys()):
                try:
                    ip = self.worker_registry[id]['ip']
                    port = self.worker_registry[id]['port']
                    with socket.create_connection((ip, port), timeout=2) as conn:
                        conn.sendall((config.HEARTBEAT_MESSAGE + "\n").encode())
                        with conn.makefile("r") as reader:
                            result = reader.readline().strip()
                        if result == "ALIVE":
                            failed_heartbeats[id] = 0
                        else:
                            print("Heartbeat result from leader was not ALIVE, but", result)
                            failed_heartbeats[id] += 1
                except Exception as e:
                    failed_heartbeats[id] += 1
                    print(f"Failed heartbeats to worker {id}:", failed_heartbeats[id])

                if failed_heartbeats[id] >= 3:
                    print(f"3 heartbeats failed to worker {id}, re assigning auctions")
                    
                    # First, remove all auctions from auction_map for this dead worker
                    # This prevents clients from getting stale worker info during recovery
                    with self.lock:
                        auctions_to_remove = [aid for aid, wid in self.auction_map.items() if wid == id]
                        for auction_id in auctions_to_remove:
                            del self.auction_map[auction_id]
                            print(f"[Leader] Removed auction {auction_id} from map (worker {id} dead)")
                    
                    # Attempt to recover auctions from CSV
                    self._reassign_auctions_with_recovery(id)
                    
                    auctions = self.worker_registry[id]['active_auctions']
                    del self.worker_registry[id]

                    for auction in auctions:
                        self._allocate_worker(auction)
