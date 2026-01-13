import socket
import random
import time
import threading
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

        while self.running:
            choice = input("\nOptions:\n  [1] List existing auctions\n  [2] Create a new auction\n> ")

            choice = choice.strip()
            if not choice.isdigit():                
                continue

            if choice == "1":
                print("List existing auctions")
                self.list_existing_auctions()
            
            elif choice == "2":
                print("Create new auction")
                self.create_new_auction()

    def list_existing_auctions(self):
        
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.sendto(config.GET_AUCTIONS_MESSAGE.encode(), (self.global_leader_ip, config.BROADCAST_PORT))

        try:
            data, addr = client_sock.recvfrom(1024)
        except Exception as e:
            print("Failed to get auctions from global leader:", e)
            return
        
        auctions = data.decode()

        print("Auctions:", auctions)

    def create_new_auction(self):
        
        item = input("Item to sell: ")
        price = input("Starting bid: ")

        message = ":".join([
            config.CREATE_AUCTION_MESSAGE,
            item,
            price
        ])

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.sendto(message.encode(), (self.global_leader_ip, config.BROADCAST_PORT))

        try:
            data, addr = client_sock.recvfrom(1024)
        except Exception as e:
            print("Failed to get auctions from global leader:", e)
            return
        
        result = data.decode()

        if result == "SUCCESS":
            print("Auction created successfully")
        else:
            print("Failed to create auction, message from global leader was:", result)


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
