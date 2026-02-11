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

        self.heartbeat_worker_thread = threading.Thread(target=self.heartbeat_worker_server, daemon=True)
        self.heartbeat_worker_thread.start()

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

    def _reassign_auctions_with_recovery(self, dead_worker_id):
        """When a worker fails, spawn a fresh worker and recover auctions to it."""
        print(f"[Leader] Attempting to recover auctions from dead worker {dead_worker_id}")
        
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
