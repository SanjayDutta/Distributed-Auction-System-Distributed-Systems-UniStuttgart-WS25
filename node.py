import socket
import threading
import time
import random
import uuid
import hashlib
import config
from ring import Ring
from leader import LeaderService
from worker import WorkerService

class Node:
    def __init__(self):
        #self.node_id = str(uuid.uuid4()) # Unique ID for this node instance
       
        randomNo = random.randint(10000000, 99999999) 
        self.node_id = str(randomNo)
        

        self.ip = socket.gethostbyname(socket.gethostname())
        self.peers = {} # Dictionary of UUID -> IP
        self.running = True
        self.running = True
        self.is_leader = False
        self.current_leader_ip = None # IP of the elected leader
        self.ring = None # Store ring instance
        self.leader_service = None
        self.worker_service = None
        
        # HS Algorithm State
        self.is_candidate = True 
        self.phase = 0
        self.replies_received = {'left': False, 'right': False}
        self.restart_election = False # Flag to trigger restart
        
        # Setup UDP socket for listening (The "Ear")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind(('', config.BROADCAST_PORT))

    def start(self):
        """Start the listener thread and the main logic loop."""
        # 1. Start the Listener Thread
        listener_thread = threading.Thread(target=self.listen_for_broadcasts, daemon=True)
        listener_thread.start()
        print(self.node_id)
        print(f"[Node {self.node_id[:8]}] Listener started on port {config.BROADCAST_PORT}")

        # 2. Run Main Logic (The "Mouth")
        self.find_leader_or_peers()
        
        # 3. Keep connection alive (and watch for Restart Signal)
        while self.running:
            if self.restart_election:
                self.restart_election = False
                self.reset_state()
                self.find_leader_or_peers()
            time.sleep(0.1)

    def reset_state(self):
        """Resets node state to clean slate for new election."""
        print("\n[System] !!! RESETTING STATE -> RESTARTING DISCOVERY !!!")
        self.peers.clear()
        self.ring = None
        self.is_candidate = True
        self.phase = 0
        self.replies_received = {'left': False, 'right': False}
        self.is_leader = False
        if hasattr(self, 'left_neighbor_uuid'): del self.left_neighbor_uuid
        if hasattr(self, 'right_neighbor_uuid'): del self.right_neighbor_uuid

    def calculate_peer_hash(self):
        """Calculates SHA256 hash of sorted list of all peer UUIDs (including self)."""
        # Gather all nodes: self + peers
        all_uuids = list(self.peers.keys())
        all_uuids.append(self.node_id)
        all_uuids.sort()
        
        # Create a single string signature
        consensus_string = ",".join(all_uuids)
        return hashlib.sha256(consensus_string.encode()).hexdigest()

    def listen_for_broadcasts(self):
        """Background thread to handle incoming messages."""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode('utf-8')
                sender_ip = addr[0]
                
                parts = message.split(":")
                msg_type = parts[0]
                
                # Check for self-message (Sender UUID usually at index 1)
                sender_uuid_extracted = None
                if len(parts) > 1:
                    sender_uuid_extracted = parts[1]
                
                if sender_uuid_extracted == self.node_id:
                    continue

                # --- HS ALGORITHM HANDLERS ---
                if msg_type == config.HS_PROBE_PREFIX:
                    # Format: HS_PROBE:SENDER:TARGET:ORIGIN:PHASE:HOPS:MAX:DIR:PEER_HASH
                    if len(parts) >= 9:
                        target_uuid = parts[2]
                        if target_uuid == self.node_id: #only listens to broadcast mesages if target = selfID, else it ignores
                            origin_uuid = parts[3]
                            phase = int(parts[4])
                            hops = int(parts[5])
                            max_hops = int(parts[6])
                            direction = parts[7]
                            peer_hash = parts[8]
                            self.handle_hs_probe(sender_uuid_extracted, origin_uuid, phase, hops, max_hops, direction, peer_hash)
                            
                elif msg_type == config.HS_REPLY_PREFIX:
                    # Format: HS_REPLY:SENDER:TARGET:ORIGIN:PHASE:DIR
                    if len(parts) >= 6:
                        target_uuid = parts[2]
                        if target_uuid == self.node_id:
                            origin_uuid = parts[3]
                            phase = int(parts[4])
                            direction = parts[5]
                            self.handle_hs_reply(origin_uuid, phase, direction)

                elif msg_type == config.ELECTION_RESTART_PREFIX:
                    if not self.restart_election:
                         print(f"[Listener] Received ELECTION_RESTART from {sender_ip}. Stopping everything.")
                         self.restart_election = True

                # --- DISCOVERY HANDLERS ---
                elif msg_type == config.DISCOVERY_MESSAGE:
                    if self.is_leader:
                        print(f"[Listener] Discovery request from {sender_ip}. Im leader, replying.")
                        response = f"{config.LEADER_FOUND_PREFIX}:{self.node_id}:{self.ip}"
                        self.sock.sendto(response.encode(), ('<broadcast>', config.BROADCAST_PORT))
                
                elif msg_type == config.PEER_DISCOVERY_PREFIX:
                    if len(parts) >= 2:
                        peer_uuid = parts[1]
                        peer_ip = sender_ip 
                        
                        if peer_uuid not in self.peers:
                            self.peers[peer_uuid] = peer_ip
                            print(f"[Listener] Hey someone is trying to discover for peers via Broadcast: {peer_ip} (UUID: {peer_uuid[:8]})")
                    
                            # Reply only if new
                            print(f"[Listener] Sending response to {sender_ip}...")
                            response = f"{config.PEER_RESPONSE_PREFIX}:{self.node_id}:{self.ip}:{peer_uuid[:8]}"
                            self.sock.sendto(response.encode(), ('<broadcast>', config.BROADCAST_PORT))
                       
                            #

                elif msg_type == config.PEER_RESPONSE_PREFIX:
                    if len(parts) >= 4:
                        peer_uuid = parts[1]
                        peer_ip = parts[2]
                        target_uuid = parts[3]
                        if peer_uuid not in self.peers:
                            if target_uuid == self.node_id:
                                self.peers[peer_uuid] = peer_ip
                                print(f"[Listener] Hey someone responded to my discovery message. New Peer Discovered via Response: {peer_ip} (UUID: {peer_uuid[:8]})")
                            else:
                                #I intercepted a response meant to a different node 
                                #this response was sent by a node that i dont know
                                #I will add this node to my peer list
                                #and also inform this node that i am here, even though this node may not be in discovery phase
                                self.peers[peer_uuid] = peer_ip
                                print(f"[Listener] Hey I intercepted a response meant to a different node. New Peer Discovered via Response: {peer_ip} (UUID: {peer_uuid[:8]})\nAdded the new peer to list and responding to their discovery message\n")
                                response = f"{config.PEER_RESPONSE_PREFIX}:{self.node_id}:{self.ip}:{peer_uuid[:8]}"
                                self.sock.sendto(response.encode(), ('<broadcast>', config.BROADCAST_PORT))
                        else:
                            print(f"[Listener] Hey someone responded to my discovery message. New Peer Discovered via Response: {peer_ip} (UUID: {peer_uuid[:8]})")
                            #but this peer id is in my list already
                            #so no need to respond to this peer
                            print("But this peer id is in my list already\n")

                elif msg_type == config.LEADER_FOUND_PREFIX:
                    # Format: LEADER_FOUND:LEADER_UUID:LEADER_IP
                    if len(parts) >= 3:
                        leader_uuid = parts[1]
                        leader_ip = parts[2]
                        print(f"\n[System] ELECTION FINISHED. Leader is {leader_ip} (UUID: {leader_uuid[:8]})")
                        self.current_leader_ip = leader_ip
                        self.is_candidate = False
                        self.is_leader = False # Ensure I don't think I'm leader if someone else won
                        
                        # If I am not leader, I should be a client/worker
                        if leader_uuid != self.node_id:
                             self.start_worker_service()
                        
                elif msg_type in (
                    config.GET_AUCTIONS_MESSAGE,
                    config.CREATE_AUCTION_MESSAGE,
                    config.JOIN_AUCTION_MESSAGE,
                    config.CLIENT_HELLO_MESSAGE,
                ):
                    if self.is_leader and self.leader_service:
                        response = self.leader_service.handle_udp_request(msg_type, parts)
                        if response:
                            self.sock.sendto(response.encode(), addr)

            except Exception as e:
                print(f"[Listener Error] {e}")

    def find_leader_or_peers(self):
        """Main Phase 1: Try to find a leader. If not, find peers."""
        print("--- Step 1: Searching for an existing Leader ---")
        self.sock.sendto(config.DISCOVERY_MESSAGE.encode(), ('<broadcast>', config.BROADCAST_PORT))
        time.sleep(2) 
        
        # Check if leader was found during sleep
        if self.current_leader_ip and not self.is_leader:
             print(f"[Discovery] Leader found at {self.current_leader_ip}. Stopping discovery.")
             self.start_worker_service()
             return

        # START PEER DISCOVERY
        print("--- Step 2: No Leader found. I discover for Peers... ---")
        start_time = time.time()
        while time.time() - start_time < 5: 
            print("I am still in discovery mode, trying find new peers") # Reduced spam
            msg = f"{config.PEER_DISCOVERY_PREFIX}:{self.node_id}"
            self.sock.sendto(msg.encode(), ('<broadcast>', config.BROADCAST_PORT))
            time.sleep(1)
        
        print(f"--- Discovery Finished. Peers found: {len(self.peers)} ---")
        
        if not self.peers:
            print("No peers found. I am alone.")
            self.become_leader()
        else:
            print("Peers found! Proceeding to Ring Formation & Election...")
            self.start_election()

    def start_election(self):
        if self.ring is not None:
            print("Ring already initialized. Skipping election.")   
            
            return # Already initialized (maybe by Listener JIT)

        print("--- Step 3: Forming Ring ---")
        self.ring = Ring(self.node_id, self.peers)
        self.left_neighbor_uuid, self.right_neighbor_uuid = self.ring.get_neighbors()
        print(f"[Node Logic] Ring Ready. Left: {self.left_neighbor_uuid[:8]}, Right: {self.right_neighbor_uuid[:8]}")
        
        # Start HS Phase 0
        self.phase = 0
        print(f"[HS Algo] Starting Election Phase {self.phase}. Probing neighbors...")
        self.start_hs_phase()

    def start_hs_phase(self):
        max_hops = 2 ** self.phase
        self.replies_received = {'left': False, 'right': False}
        
        # Send Probes (Left and Right)
        # Direction implies logical direction.
        self._send_probe(self.left_neighbor_uuid, direction='left', hops=1, max_hops=max_hops)
        self._send_probe(self.right_neighbor_uuid, direction='right', hops=1, max_hops=max_hops)

    def _send_probe(self, target_uuid, direction, hops, max_hops):
        # Format: HS_PROBE:SENDER:TARGET:ORIGIN:PHASE:HOPS:MAX:DIR:PEER_HASH
        peer_hash = self.calculate_peer_hash()
        msg = f"{config.HS_PROBE_PREFIX}:{self.node_id}:{target_uuid}:{self.node_id}:{self.phase}:{hops}:{max_hops}:{direction}:{peer_hash}"
        self.sock.sendto(msg.encode(), ('<broadcast>', config.BROADCAST_PORT))

    def _send_probe_forward(self, target_uuid, origin, phase, hops, max_hops, direction, peer_hash):
        msg = f"{config.HS_PROBE_PREFIX}:{self.node_id}:{target_uuid}:{origin}:{phase}:{hops}:{max_hops}:{direction}:{peer_hash}"
        self.sock.sendto(msg.encode(), ('<broadcast>', config.BROADCAST_PORT))
        
    def _send_reply(self, target_uuid, origin, phase, direction):
        # Format: HS_REPLY:SENDER:TARGET:ORIGIN:PHASE:DIR
        msg = f"{config.HS_REPLY_PREFIX}:{self.node_id}:{target_uuid}:{origin}:{phase}:{direction}"
        self.sock.sendto(msg.encode(), ('<broadcast>', config.BROADCAST_PORT))

    def handle_hs_probe(self, sender_uuid, origin_uuid, phase, hops, max_hops, direction, peer_hash):
        # 0. GUARD: Check Consistency
        local_hash = self.calculate_peer_hash()
        if peer_hash != local_hash:
            print(f"[HS Guard] Mismatch! My Hash: {local_hash[:8]} vs Incoming: {peer_hash[:8]}")
            print("[HS Guard] Broadcasting RESTART and resetting myself.")
            
            # Broadcast Global Restart
            msg = f"{config.ELECTION_RESTART_PREFIX}:{self.node_id}"
            self.sock.sendto(msg.encode(), ('<broadcast>', config.BROADCAST_PORT))
            
            # Trigger Local Restart
            self.restart_election = True
            return

        # 0.5 JIT Initialization: Valid Hash but I haven't formed ring yet? Do it now!
        if self.ring is None:
             print("[HS Guard] Peer Hash Valid but Ring not ready. Doing JIT Initialization...")
             self.start_election()

        # Logic: 
        # 1. If Origin == Me -> WIN
        # 2. If Origin > Me -> Relay
        # 3. If Origin < Me -> Swallow
        
        if origin_uuid == self.node_id:
            print(f"!!! [HS] MY PROBE RETURNED! I AM THE LEADER !!!")
            self.become_leader()
            return
            
        if self.is_candidate and origin_uuid > self.node_id:
             self.is_candidate = False # Beaten by superior ID
             print(f"[HS] I am passive now. (Superior: {origin_uuid[:8]})")
             
        if not self.is_candidate or origin_uuid > self.node_id:
            # RELAY logic
            if hops < max_hops:
                print(f"[HS] Relaying Probe from {origin_uuid[:8]} (Hops: {hops}/{max_hops})")
                next_hops = hops + 1
                next_target = self.left_neighbor_uuid if direction == 'left' else self.right_neighbor_uuid
                self._send_probe_forward(next_target, origin_uuid, phase, next_hops, max_hops, direction, peer_hash)
            else:
                # REPLY logic (Turn around)
                print(f"[HS] Max Hops Reached. Replying to {origin_uuid[:8]}")
                # Send back to the node who sent it to me (reverse path)
                # Logic: If dir='left', probe came from right, so reply to right?
                # No, if dir='left', probe is traveling LEFT. So it came from my RIGHT neighbor.
                # So I reply to my RIGHT neighbor.
                reply_target = self.right_neighbor_uuid if direction == 'left' else self.left_neighbor_uuid
                self._send_reply(reply_target, origin_uuid, phase, direction)
        else:
            # origin < self.node_id: Swallow
             print(f"[HS] Swallowing Probe from Inferior {origin_uuid[:8]}")

    def handle_hs_reply(self, origin_uuid, phase, direction):
        # Logic: Relay reply back to origin.
        # If I AM origin, count it.
        
        if origin_uuid == self.node_id:
            print(f"[HS] Received REPLY from direction: {direction}")
            self.replies_received[direction] = True
            
            if self.replies_received['left'] and self.replies_received['right']:
                print(f"[HS] Phase {self.phase} Complete! Starting Next Phase...")
                self.phase += 1
                self.start_hs_phase()
        else:
            # Relay Reply
            print(f"[HS] Relaying REPLY for {origin_uuid[:8]}")
            # Reply travels Opposite to Probe direction.
            # If Probe dir='left', Reply travels RIGHT.
            # So I send to my Right neighbor?
            # Yes.
            next_target = self.right_neighbor_uuid if direction == 'left' else self.left_neighbor_uuid
            self._send_reply(next_target, origin_uuid, phase, direction)

    def become_leader(self):
        if not self.is_leader:
            self.is_leader = True
            print(">>> I AM THE LEADER NOW <<<")
            self.current_leader_ip = self.ip
            self.leader_service = LeaderService(self.ip)
            self.leader_service.start()
            
            # Announce leadership to the world
            print(f"[Leader] Announcing victory to all nodes...")
            msg = f"{config.LEADER_FOUND_PREFIX}:{self.node_id}:{self.ip}"
            self.sock.sendto(msg.encode(), ('<broadcast>', config.BROADCAST_PORT))
            
            # Start Server Logic here...

    def start_worker_service(self):
        if not self.current_leader_ip:
            return
        if not self.worker_service:
            self.worker_service = WorkerService(self.node_id, self.ip)
            self.worker_service.start(self.current_leader_ip)

if __name__ == "__main__":
    node = Node()
    try:
        node.start()
    except KeyboardInterrupt:
        print("\nNode stopping...")
        node.running = False
