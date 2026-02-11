import socket
import random
import time
import threading
import json
import argparse
import requests

import config

class Client:
    def __init__(self, leader_ip=None):
        # Parse command line arguments if leader_ip not provided
        if leader_ip is None:
            parser = argparse.ArgumentParser(description='Auction Client')
            parser.add_argument('--leader', required=True, help='Leader machine IP address')
            args = parser.parse_args()
            leader_ip = args.leader
        
        self.leader_url = f"http://{leader_ip}:{config.LEADER_HTTP_PORT}"
        
        randomNo = random.randint(10000000, 99999999) 
        self.client_id = str(randomNo)
        
        self.running = True
        self.auction_sock = None
        self.auction_running = False
        self.auction_sequence_number = 0
        
        # Recovery state
        self.current_auction_id = None
        self.recovery_retries = 0
        self.max_recovery_retries = 10
        self.recovery_retry_delay = 5

    def start(self):
        print(f"[Client {self.client_id[:8]}] Connecting to leader at {self.leader_url}")
        
        # Test connection to leader
        try:
            auctions = self.list_existing_auctions()
        except Exception as e:
            print(f"\nFailed to connect to leader at {self.leader_url}: {e}")
            print("Make sure:")
            print("  1. Node processes are running on the leader machine")
            print("  2. Leader has been elected")
            print("  3. The IP address is correct")
            return
        
        print("Connected to leader successfully!\n")

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
        try:
            response = requests.get(f"{self.leader_url}/auctions", timeout=5)
            response.raise_for_status()
            auctions = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to get auctions from leader: {e}")
            return {}
        except Exception as e:
            print(f"Error processing auction list: {e}")
            return {}
        
        auction_input_map = {}

        if not auctions:
            print("\nNo auctions available")
        else:
            for i, key in enumerate(auctions.keys()):
                auction_input_map[i] = {"auction_id": key, **auctions[key]}
                print(f"  [{i}] {auction_input_map[i]['item']} - Highest bid: {auction_input_map[i]['highest_bid']}")
            
        return auctions

    def create_new_auction(self):
        item = input("Item to sell: ")
        price = input("Starting bid: ")

        while not price.isdigit():
            price = input("Enter a real number for starting bid: ")

        try:
            response = requests.post(
                f"{self.leader_url}/auctions",
                json={"item": item, "starting_bid": price},
                timeout=5
            )
            response.raise_for_status()
            result = response.json()
            
            if "auction_id" in result:
                print("Auction created successfully")
                self.highest_bid = price
                self.join_auction(
                    result["auction_id"],
                    result["worker_ip"],
                    result["worker_port"]
                )
            else:
                print("Failed to create auction:", result)
        except requests.exceptions.RequestException as e:
            print(f"Failed to create auction: {e}")
        except Exception as e:
            print(f"Error creating auction: {e}")

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
        
        # Query leader via HTTP for current auction location
        try:
            response = requests.get(
                f"{self.leader_url}/auctions/{self.current_auction_id}",
                timeout=5
            )
            response.raise_for_status()
            result = response.json()
            
            if "worker_ip" in result:
                worker_ip = result["worker_ip"]
                worker_port = result["worker_port"]
                
                print(f"[RECOVERY] Found auction on worker: {worker_ip}:{worker_port}")
                
                # Close old connection
                try:
                    self.auction_sock.close()
                except:
                    pass
                
                # Reconnect to new worker
                self._reconnect_to_worker(self.current_auction_id, worker_ip, worker_port)
            else:
                print(f"[RECOVERY] Auction not found on leader")
        except requests.exceptions.RequestException as e:
            print(f"[RECOVERY] Failed to query leader: {e}")
        except Exception as e:
            print(f"[RECOVERY] Error during recovery: {e}")

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


if __name__ == "__main__":
    client = Client()
    client.start()
