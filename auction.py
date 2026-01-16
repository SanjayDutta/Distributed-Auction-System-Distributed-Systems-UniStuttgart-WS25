import time
import uuid


class Auction:
    # Auction: id, item, starting_bid, highest_bid, highest_bidder, status, end_time, participants
    def __init__(self, item, starting_bid, duration_seconds=60, auction_id=None, start_time=None):
        self.id = auction_id or str(uuid.uuid4())
        self.item = item
        self.starting_bid = float(starting_bid)
        self.highest_bid = self.starting_bid
        self.highest_bidder = None
        self.status = "open"
        self.start_time = start_time if start_time is not None else time.time()
        self.end_time = (
            self.start_time + duration_seconds
            if duration_seconds is not None
            else None
        )
        self.participants = set()

    def is_open(self):
        if self.status != "open":
            return False
        if self.end_time is None:
            return True
        return time.time() < self.end_time

    def add_participant(self, bidder):
        if bidder is not None:
            self.participants.add(bidder)

    def place_bid(self, bidder, amount):
        if not self.is_open():
            return False

        try:
            bid_amount = float(amount)
        except (TypeError, ValueError):
            return False

        if bid_amount < self.starting_bid:
            return False
        if self.highest_bidder is not None and bid_amount <= self.highest_bid:
            return False

        self.highest_bid = bid_amount
        self.highest_bidder = bidder
        self.add_participant(bidder)
        return True

    def get_highest_bid(self):
        if self.highest_bidder is None:
            return None
        return self.highest_bidder, self.highest_bid

    def close(self):
        self.status = "closed"
