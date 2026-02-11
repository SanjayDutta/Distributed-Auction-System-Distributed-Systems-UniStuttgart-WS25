import socket
import random
import time
import threading
import json

import config

class Client:
    def __init__(self):
        randomNo = random.randint(10000000, 99999999) 
        self.client_id = str(randomNo)
        self.global_leader_ip = None
        
        # Setup UDP socket for listening (The "Ear")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind(('', config.BROADCAST_PORT))

        self.running = True

        self.auction_sock = None

        self.auction_running = False

        self.auction_sequence_number = 0
        
        # Recovery state
        self.current_auction_id = None
        self.recovery_retries = 0
        self.max_recovery_retries = 10  # Increased to allow more time for leader recovery
        self.recovery_retry_delay = 5  # seconds between retries (increased for spawn time)

    def start(self):
        listener_thread = threading.Thread(target=self.listen_for_broadcasts, daemon=True)
        listener_thread.start()
        # print(self.node_id)
        print(f"[Client {self.client_id[:8]}] Listener started on port {config.BROADCAST_PORT}")
        # time.sleep(1)

        self.sock.sendto(config.DISCOVERY_MESSAGE.encode(), ('<broadcast>', config.BROADCAST_PORT))
        
        print("Sent discovery, waiting until we find the leader")

        while not self.global_leader_ip:
            time.sleep(1)
        
        auctions = self.list_existing_auctions()
        
        auction_input_map = {}

        while self.running:
            choice = input("\nOptions:\n  [1] List existing auctions\n  [2] Join auction\n  [3] Create a new auction\n> ")

            choice = choice.strip()
            if not choice.isdigit():                
                continue

            if choice == "1":
                print("List existing auctions")
                auctions = self.list_existing_auctions()
            
            elif choice == "2":
                if not auctions:
                    continue
                print("Join auction")

                auction_input = input("Select auction number: ")

                if not auction_input.isdigit():
                    continue

                if auctions is None or not len(auctions.keys()):
                    print("\nNo auctions available")
                    continue

                for i, key in enumerate(auctions.keys()):
                    auction_input_map[i] = {"auction_id": key, **auctions[key]}
                    print(f"  [{i}]", auction_input_map[i]['item'], auction_input_map[i]['highest_bid'])
        
                auction_index = int(auction_input)
                print(auction_input_map)

                if auction_index not in auction_input_map:
                    continue

                auction_to_join = auction_input_map[auction_index]

                self.highest_bid = auction_to_join['highest_bid']
                self.join_auction(auction_to_join['auction_id'], auction_to_join['worker_ip'], auction_to_join['worker_port'])

            elif choice == "3":
                print("Create new auction")
                self.create_new_auction()

    def list_existing_auctions(self):
        
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.settimeout(5)
        client_sock.sendto(config.GET_AUCTIONS_MESSAGE.encode(), (self.global_leader_ip, config.BROADCAST_PORT))
        print("wait for result")
        try:
            data, addr = client_sock.recvfrom(1024)
        except Exception as e:
            print("Failed to get auctions from global leader:", e)
            return
        
        message = data.decode()
        parts = message.split(":", 1)
        result = parts[0]

        if result != "AUCTION_LIST":
            print("Failed to get the auction list, response was:", message)

        if len(parts) >= 2:
            auctions = json.loads(parts[1])

        auction_input_map = {}

        if auctions is None or not len(auctions.keys()):
            print("\nNo auctions available")
        else:
            for i, key in enumerate(auctions.keys()):
                auction_input_map[i] = {"auction_id": key, **auctions[key]}
                print(f"  [{i}]", auction_input_map[i]['item'], auction_input_map[i]['highest_bid'])
            
        return auctions

    def create_new_auction(self):
        
        item = input("Item to sell: ")
        price = input("Starting bid: ")

        while not price.isdigit():
            price = input("Enter a real number for starting bid: ")

        message = ":".join([
            config.CREATE_AUCTION_MESSAGE,
            item,
            price
        ])

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.settimeout(5)
        client_sock.sendto(message.encode(), (self.global_leader_ip, config.BROADCAST_PORT))

        try:
            data, addr = client_sock.recvfrom(1024)
        except Exception as e:
            print("Failed to get auctions from global leader:", e)
            return
        
        message = data.decode('utf-8')
    
        parts = message.split(":")
        result = parts[0]
        
        # SUCCESS:978594d9-2eac-4161-98a7-32eae24d0a8a:192.168.0.80:58902
        if result == "SUCCESS":
            print("Auction created successfully")
            auction_id = parts[1]
            server_ip = parts[2]
            server_port = int(parts[3])

            # print(parts)
            self.highest_bid = price
            self.join_auction(auction_id, server_ip, server_port)

        else:
            print("Failed to create auction, message from global leader was:", message)

    def join_auction(self, auction_id, server_ip, server_port):

        message = config.AUCTION_JOIN_MESSAGE + ":" + auction_id + ":" + self.client_id + "\n"
        
        print("try join auction", message)
        
        # Store current auction ID for recovery purposes
        self.current_auction_id = auction_id
        self.recovery_retries = 0

        try:
            auction_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            auction_sock.settimeout(5)
            auction_sock.connect((server_ip, server_port))

            auction_sock.sendall(message.encode())

            data = auction_sock.recv(1024)
        except Exception as e:
            print("Failed to join auction:", e)
            return
        
        msg = data.decode('utf-8')
    
        parts = msg.strip().split(":")
        result = parts[0]

        print(parts)
        if result != "OK":
            print("Failed to join auction, response was:", msg)
            return
        
        if len(parts) < 2:
            return
        self.auction_sequence_number = int(parts[1])

        self.auction_sock = auction_sock
        
        self.auction_running = True
        listener_thread = threading.Thread(target=self.listen_for_auction_messages, daemon=True)
        listener_thread.start()

        while self.auction_running:
            new_bid = input(f"Add bid (current: {self.highest_bid}): ")

            if not new_bid.isdigit():
                continue

            if float(new_bid) < float(self.highest_bid):
                continue

            message = ":".join([
                config.AUCTION_BID_MESSAGE,
                auction_id,
                self.client_id,
                str(new_bid),
                str(self.auction_sequence_number + 1)
            ]) + "\n"

            print("sending...")
            try:
                auction_sock.sendall(message.encode())
            except Exception as e:
                print("Failed to send message:", e)
                return

            # try:
            #     response = auction_sock.recv(1024).decode('utf-8')
            # except Exception as e:
            #     print("Failed to send highest bid:", e)
            #     continue
            
            # print(response)

            # parts = response.strip().split(":")
            # result = parts[0]

            # if len(parts) < 2:
            #     continue
            
            # if result == "OK":
            #     print("New highest bid: ", parts[1])
            #     self.highest_bid = float(parts[1])

            # elif result == "REJECT" and parts[1] == "LOW_BID":
            #     print(f"Bid {new_bid} was too low!")

            # if len(parts) < 3:
            #     continue
            
            # if result == "AUCTION_BID_UPDATE":
            #     print(f"New highest bid:", parts[2])
            #     highest_bid = float(parts[2])



    def listen_for_auction_messages(self):
        """Listen for auction messages with automatic recovery on connection failure."""
        
        while self.auction_running:
            try:
                data = self.auction_sock.recv(1024)
                if not data:
                    # Socket closed by server
                    raise ConnectionResetError("Server closed connection")
            except socket.timeout:
                # Timeout is normal if no messages arrive; just continue waiting
                continue
            except (ConnectionResetError, BrokenPipeError, socket.error) as e:
                print(f"\n[ERROR] Connection to worker lost: {e}")
                self._attempt_recovery()
                return  # Exit listener, main thread will handle recovery
            except Exception as e:
                continue

            try:
                message = data.decode('utf-8').strip()
                parts = message.split(":")
                result = parts[0]

                if result == config.AUCTION_BID_UPDATE_MESSAGE:
                    if len(parts) < 4:
                        continue

                    self.highest_bid = parts[2]
                    highest_bidder = parts[3]
                    self.auction_sequence_number = int(parts[4])

                    print(f"\n{highest_bidder} just made the highest bid {self.highest_bid}!")
                    
                elif result == "OK":
                    if len(parts) < 3:
                        continue
                    self.highest_bid = parts[1]
                    self.auction_sequence_number = int(parts[2])

                    print("\nYour bid was accepted!")
                    print("New highest bid:", self.highest_bid)

                elif result == "REJECT":
                    if parts[1] == "LOW_BID":
                        print(f"\nYour bid was too low!")
                    elif parts[1] == "MSG_OUT_OF_ORDER":
                        print("Someone else sent a bid earlier!")

                elif result == config.AUCTION_RESULT_MESSAGE:

                    if len(parts) < 4:
                        continue
                    winner = parts[2]
                    winning_bid = parts[3]
                    if winner == self.client_id:
                        winner += " (you)"

                    print("\n**********\n\nAuction has finished!\n")
                    print("Winner:", winner, "with the bid", winning_bid)
                    self.auction_running = False
            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")
                continue

    def _attempt_recovery(self):
        """Attempt to reconnect to a recovered auction worker."""
        if self.recovery_retries >= self.max_recovery_retries:
            print(f"\n[FATAL] Recovery failed after {self.max_recovery_retries} attempts. Giving up.")
            self.auction_running = False
            return
        
        self.recovery_retries += 1
        print(f"\n[RECOVERY] Attempting to recover (attempt {self.recovery_retries}/{self.max_recovery_retries})...")
        
        # Wait before attempting recovery to give leader time to detect failure and reassign
        print(f"[RECOVERY] Waiting {self.recovery_retry_delay} seconds before retry...")
        time.sleep(self.recovery_retry_delay)
        
        # Query leader for current auction location
        try:
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_sock.settimeout(5)
            
            # Reuse JOIN_AUCTION_MESSAGE to get current worker location
            message = f"{config.JOIN_AUCTION_MESSAGE}:{self.current_auction_id}"
            client_sock.sendto(message.encode(), (self.global_leader_ip, config.BROADCAST_PORT))
            
            data, addr = client_sock.recvfrom(1024)
            response = data.decode('utf-8')
            parts = response.split(":")
            
            if response.startswith("WORKER_INFO:"):
                # Format: WORKER_INFO:auction_id:worker_ip:worker_port
                auction_id = parts[1]
                worker_ip = parts[2]
                worker_port = int(parts[3])
                
                print(f"[RECOVERY] Found auction on new worker: {worker_ip}:{worker_port}")
                
                # Close old connection
                try:
                    self.auction_sock.close()
                except:
                    pass
                
                # Reconnect to new worker
                self._reconnect_to_worker(auction_id, worker_ip, worker_port)
            else:
                print(f"[RECOVERY] Unexpected response from leader: {response}")
        except Exception as e:
            print(f"[RECOVERY] Failed to query leader: {e}")

    def _reconnect_to_worker(self, auction_id, worker_ip, worker_port):
        """Reconnect to a (potentially new) worker and rejoin the auction."""
        try:
            print(f"[RECOVERY] Connecting to worker at {worker_ip}:{worker_port}")
            
            auction_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            auction_sock.settimeout(5)
            auction_sock.connect((worker_ip, worker_port))
            
            # Send join message
            message = f"{config.AUCTION_JOIN_MESSAGE}:{auction_id}:{self.client_id}\n"
            auction_sock.sendall(message.encode())
            
            data = auction_sock.recv(1024)
            msg = data.decode('utf-8').strip()
            parts = msg.split(":")
            
            if parts[0] == "OK":
                self.auction_sequence_number = int(parts[1])
                self.auction_sock = auction_sock
                print("[RECOVERY] Successfully rejoined auction! Resuming...")
                
                # Restart listener
                listener_thread = threading.Thread(target=self.listen_for_auction_messages, daemon=True)
                listener_thread.start()
            else:
                print(f"[RECOVERY] Failed to rejoin auction: {msg}")
                self.auction_running = False
        except Exception as e:
            print(f"[RECOVERY] Failed to reconnect: {e}")
            if self.recovery_retries < self.max_recovery_retries:
                print("[RECOVERY] Retrying...")
                self._attempt_recovery()
            else:
                self.auction_running = False



    def listen_for_broadcasts(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1024)
            except Exception as e:
                print("Failed to discover global leader:", e)
                return

            message = data.decode('utf-8')
            print("Received message:", message)
            
            if message == config.DISCOVERY_MESSAGE:
                continue

            parts = message.split(":")
            msg_type = parts[0]
            
            if msg_type == config.LEADER_FOUND_PREFIX:
                # Format: LEADER_FOUND:LEADER_UUID:LEADER_IP
                if len(parts) >= 3:
                    leader_uuid = parts[1]
                    leader_ip = parts[2]
                    print(f"\nLeader is {leader_ip} (UUID: {leader_uuid[:8]})")
                    self.global_leader_ip = leader_ip


if __name__ == "__main__":
    client = Client()
    client.start()
