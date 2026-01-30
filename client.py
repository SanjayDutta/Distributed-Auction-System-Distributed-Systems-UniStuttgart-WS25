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

        auctions = {}

        while self.running:
            choice = input("\nOptions:\n  [1] List existing auctions\n  [2] Join auction\n  [3] Create a new auction\n> ")

            choice = choice.strip()
            if not choice.isdigit():                
                continue

            if choice == "1":
                print("List existing auctions")
                auctions = self.list_existing_auctions()
            
            elif choice == "2":
                print("Join auction")

                auction_input_map = {}
                for i, key in enumerate(auctions.keys()):
                    auction_input_map[i] = {"auction_id": key, **auctions[key]}
                    print(f"  [{i}]", auction_input_map[i]['item'], auction_input_map[i]['highest_bid'])
                    
                auction_input = input("Select auction number: ")

                if not auction_input.isdigit():
                    continue

                auction_index = int(auction_input)
                print(auction_input_map)

                if auction_index not in auction_input_map:
                    continue

                auction_to_join = auction_input_map[auction_index]

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

        print("Auctions:", auctions)

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

            print(parts)
            self.join_auction(auction_id, server_ip, server_port)

        else:
            print("Failed to create auction, message from global leader was:", message)

    def join_auction(self, auction_id, server_ip, server_port):

        message = config.JOIN_AUCTION_MESSAGE + ":" + auction_id
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.settimeout(5)
        client_sock.sendto(message.encode(), (server_ip, server_port))

        try:
            data, addr = client_sock.recvfrom(1024)
        except Exception as e:
            print("Failed to join auction:", e)
            return
        
        message = data.decode('utf-8')
    
        parts = message.split(":")
        result = parts[0]

        print(parts)
        if result != "OK":
            print("Failed to join auction, response was:", message)
            return

        self.auction_sock = client_sock
        listener_thread = threading.Thread(target=self.listen_for_auction_messages, daemon=True)
        listener_thread.start()

        auction_running = True

        while auction_running:
            new_bid = input("Add bid: ")

            message = ":".join([
                config.AUCTION_BID_MESSAGE,
                auction_id,
                new_bid
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


    # def listen_for_auction_messages(self):
        
    #     while True:
    #         try:
    #             data, addr = self.auction_sock.recvfrom(1024)
    #         except Exception as e:
    #             print("Failed to listen to auction messages:", e)
    #             return


    #         message = data.decode('utf-8')
    #         parts = message.split(":")
    #         result = parts[0]

    #         print(parts)
    #         if result != "OK":
    #             print("Failed to join auction")
    #             # return


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
