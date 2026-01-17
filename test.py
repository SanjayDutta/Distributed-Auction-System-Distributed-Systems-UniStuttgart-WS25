import argparse
import json
import socket
import time

import config


class WorkerTcpClient:
    def __init__(self, host, port, timeout=2):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self._buffer = b""

    def send_line(self, line):
        self.sock.sendall((line + "\n").encode())

    def recv_line(self, timeout=2):
        self.sock.settimeout(timeout)
        while b"\n" not in self._buffer:
            chunk = self.sock.recv(4096)
            if not chunk:
                return None
            self._buffer += chunk
        line, _, rest = self._buffer.partition(b"\n")
        self._buffer = rest
        return line.decode().strip()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def _udp_request_broadcast(message, timeout=2, retries=3):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        for attempt in range(1, retries + 1):
            sock.sendto(message.encode(), ("<broadcast>", config.BROADCAST_PORT))
            try:
                data, addr = sock.recvfrom(4096)
                return data.decode(), addr
            except socket.timeout:
                if attempt == retries:
                    raise TimeoutError(
                        "UDP request timed out after retries. "
                        f"port={config.BROADCAST_PORT}, message={message}"
                    )
    finally:
        sock.close()


def udp_create_auction(item, starting_bid, creator_id=None, timeout=2):
    message = f"{config.CREATE_AUCTION_MESSAGE}:{item}:{starting_bid}"
    if creator_id:
        message += f":{creator_id}"
    response, addr = _udp_request_broadcast(message, timeout=timeout)
    if response.startswith("SUCCESS:"):
        _, auction_id, worker_ip, worker_port = response.split(":")
        return {
            "auction_id": auction_id,
            "worker_ip": worker_ip,
            "worker_port": int(worker_port),
            "leader_addr": addr,
        }
    return {"error": response}


def udp_join_auction(auction_id, timeout=2):
    message = f"{config.JOIN_AUCTION_MESSAGE}:{auction_id}"
    response, addr = _udp_request_broadcast(message, timeout=timeout)
    if response.startswith("WORKER_INFO:"):
        _, auction_id, worker_ip, worker_port = response.split(":")
        return {
            "auction_id": auction_id,
            "worker_ip": worker_ip,
            "worker_port": int(worker_port),
            "leader_addr": addr,
        }
    return {"error": response}

def udp_hello(timeout=2):
    response, _ = _udp_request_broadcast(config.CLIENT_HELLO_MESSAGE, timeout=timeout)
    prefix = f"{config.AUCTION_LIST_MESSAGE}:"
    if response.startswith(prefix):
        payload = response[len(prefix):]
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {"error": "bad_json", "raw": payload}
    return {"error": response}


def test_create_and_listen(item="car", starting_bid=100, client_id="client1"):
    result = udp_create_auction(item, starting_bid, creator_id=client_id)
    if "error" in result:
        print("create failed:", result["error"])
        return

    auction_id = result["auction_id"]
    worker_ip = result["worker_ip"]
    worker_port = result["worker_port"]
    print(f"created auction_id={auction_id} worker={worker_ip}:{worker_port}")

    client = WorkerTcpClient(worker_ip, worker_port)
    try:
        join = f"{config.AUCTION_JOIN_MESSAGE}:{auction_id}:{client_id}"
        client.send_line(join)
        print("join:", client.recv_line())

        listen_for_updates(client, duration=None)
    finally:
        client.close()


def test_bid_and_listen(auction_id, client_id="client2", bid_amount=120):
    result = udp_join_auction(auction_id)
    if "error" in result:
        print("join lookup failed:", result["error"])
        return

    worker_ip = result["worker_ip"]
    worker_port = result["worker_port"]
    print(f"bidding on auction_id={auction_id} worker={worker_ip}:{worker_port}")

    client = WorkerTcpClient(worker_ip, worker_port)
    try:
        join = f"{config.AUCTION_JOIN_MESSAGE}:{auction_id}:{client_id}"
        client.send_line(join)
        print("join:", client.recv_line())

        bid = f"{config.AUCTION_BID_MESSAGE}:{auction_id}:{client_id}:{bid_amount}"
        client.send_line(bid)
        print("bid:", client.recv_line())

        listen_for_updates(client, duration=None)
    finally:
        client.close()


def test_status(worker_ip, worker_port, auction_id):
    client = WorkerTcpClient(worker_ip, worker_port)
    try:
        msg = f"{config.AUCTION_STATUS_MESSAGE}:{auction_id}"
        client.send_line(msg)
        print("status:", client.recv_line())
    finally:
        client.close()


def listen_for_updates(client, duration=None):
    end_time = time.time() + duration if duration else None
    while True:
        if end_time and time.time() >= end_time:
            return
        remaining = 1.0 if not end_time else max(0.5, end_time - time.time())
        try:
            line = client.recv_line(timeout=remaining)
        except socket.timeout:
            continue
        if line:
            print("update:", line)


def main():
    parser = argparse.ArgumentParser(description="Auction system test client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    hello_cmd = subparsers.add_parser("hello", help="Fetch auction list from leader")

    create_cmd = subparsers.add_parser("create", help="Create an auction and listen for updates")
    create_cmd.add_argument("--item", default="car")
    create_cmd.add_argument("--price", type=float, default=100)
    create_cmd.add_argument("--client", default="client1")

    bid_cmd = subparsers.add_parser("bid", help="Bid on an auction and listen for updates")
    bid_cmd.add_argument("--auction", required=True)
    bid_cmd.add_argument("--amount", type=float, required=True)
    bid_cmd.add_argument("--client", default="client2")

    status_cmd = subparsers.add_parser("status", help="Query auction status on a worker")
    status_cmd.add_argument("--worker-ip", required=True)
    status_cmd.add_argument("--worker-port", type=int, required=True)
    status_cmd.add_argument("--auction", required=True)

    args = parser.parse_args()
    if args.command == "hello":
        auctions = udp_hello()
        print("auctions:", auctions)
        return
    if args.command == "create":
        test_create_and_listen(item=args.item, starting_bid=args.price, client_id=args.client)
        return
    if args.command == "bid":
        test_bid_and_listen(auction_id=args.auction, client_id=args.client, bid_amount=args.amount)
        return
    if args.command == "status":
        test_status(worker_ip=args.worker_ip, worker_port=args.worker_port, auction_id=args.auction)
        return
    
    
# Example usages:
# python test.py hello
# python test.py create --item car --price 100 --client client1
# python test.py bid --auction <auction_id> --amount 150 --client client2

if __name__ == "__main__":
    main()
