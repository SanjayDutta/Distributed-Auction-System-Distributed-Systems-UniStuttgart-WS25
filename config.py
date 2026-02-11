# Ports
BROADCAST_PORT = 5005
WORKER_TCP_PORT = 7001
LEADER_TCP_PORT = 7000
LEADER_HTTP_PORT = 8080

# Discovery / peer messages (UDP broadcast)
DISCOVERY_MESSAGE = "DISCOVER_AUCTION_LEADER"
PEER_DISCOVERY_PREFIX = "PEER_DISCOVERY"
PEER_RESPONSE_PREFIX = "PEER_RESPONSE"
LEADER_FOUND_PREFIX = "LEADER_FOUND"
LEADER_IP_PREFIX = "LEADER_IP"
WHO_IS_LEADER_MESSAGE = "WHO_IS_LEADER"
LEADER_RESPONSE_MESSAGE = "LEADER_IP"

# Election messages (UDP broadcast)
HS_PROBE_PREFIX = "HS_PROBE" # Format: HS_PROBE:SENDER:TARGET:ORIGIN:PHASE:HOPS:MAX:DIR:PEER_HASH
HS_REPLY_PREFIX = "HS_REPLY" # Format: HS_REPLY:SENDER:TARGET:ORIGIN:PHASE:DIR
ELECTION_RESTART_PREFIX = "ELECTION_RESTART"

# Client <-> Leader messages (UDP)
GET_AUCTIONS_MESSAGE = "GET_AUCTIONS"
CREATE_AUCTION_MESSAGE = "CREATE_AUCTION"
JOIN_AUCTION_MESSAGE = "JOIN_AUCTION"
CLIENT_HELLO_MESSAGE = "CLIENT_HELLO"
AUCTION_LIST_MESSAGE = "AUCTION_LIST"

# Leader <-> Worker control (TCP)
WORKER_REGISTER_MESSAGE = "WORKER_REGISTER"
AUCTION_DONE_MESSAGE = "AUCTION_DONE"
HEARTBEAT_MESSAGE = "LEADER_HEARTBEAT"

# Worker capacity (max concurrent auctions per worker)
WORKER_MAX_AUCTIONS = 1

# Auction protocol (Worker <-> Client, TCP)
# AUCTION_BID_UPDATE_MESSAGE is also reused for worker -> leader bid updates.
AUCTION_CREATE_MESSAGE = "AUCTION_CREATE"
AUCTION_JOIN_MESSAGE = "AUCTION_JOIN"
AUCTION_BID_MESSAGE = "AUCTION_BID"
AUCTION_BID_UPDATE_MESSAGE = "AUCTION_BID_UPDATE"
AUCTION_STATUS_MESSAGE = "AUCTION_STATUS"
AUCTION_RESULT_MESSAGE = "AUCTION_RESULT"


def get_network_ip():
    """
    Get the IP address of the machine on the local network.
    Returns the IP that would be used to connect to an external address.
    Falls back to hostname resolution if necessary.
    """
    import socket
    try:
        # Create a socket and try to connect to a remote address
        # We don't actually connect, just use it to determine local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # Google DNS
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        # Fallback: try hostname resolution
        try:
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            # Avoid returning localhost
            if ip != "127.0.0.1":
                return ip
        except Exception:
            pass
    
    # Last resort: return localhost (won't work across networks)
    return "127.0.0.1"
