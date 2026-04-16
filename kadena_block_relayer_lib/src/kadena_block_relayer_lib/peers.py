from collections import deque
import random
import pickle
import logging
from contextlib import contextmanager


from .exceptions import Missingdata
from .bootstrap_nodes import BOOTSTRAPS

logger = logging.getLogger(__name__)


class PeersDb:
    def __init__(self, network):
        self.queue = deque()
        self.peers = None
        self.bootstraps = BOOTSTRAPS[network]
        self.db_file = "peers_{}.db".format(network)
        self.load()

    def save(self):
        try:
            with open(self.db_file, "wb") as f:
                pickle.dump(self.peers, f)
                logger.info("Peer DB saved with {:d} peers".format(len(self.peers)))
        except Exception:
            pass

    def load(self):
        try:
            logger.info("Loading peer DB")
            with open(self.db_file, "rb") as f:
                self.peers = pickle.load(f)

            self.queue.clear()
            self.queue.extend(random.sample(list(self.peers), len(self.peers)))
            logger.info("{:d} peers".format(len(self.peers)))
        except Exception:
            logger.info("Peer DB unavailable")
            self.peers = set()

    def __contains__(self, item):
        return item in self.peers

    def __len__(self):
        return len(self.peers)

    def insert(self, p):
        if p not in self.peers:
            logger.info("Adding new peer {!s}".format(p))
            self.queue.append(p)
            self.peers.add(p)

    @property
    def fallback_peer(self):
        return random.choice(list(self.peers) if self.peers else self.bootstraps)

    @contextmanager
    def next_peer(self):
        try:
            peer = self.queue.popleft()
        except IndexError:
            peer = None
            logger.info("No peer available: use a fallback")

        def __enqueue():
            if peer:
                self.queue.append(peer)

        try:
            yield peer if peer is not None else self.fallback_peer
            __enqueue()
        except Missingdata:
            # In case it's a mssing data error, it's not fatal, just put back in queue
            __enqueue()
            raise
        except Exception as e:
            if peer:
                logger.info("Removing peer {!s} because of: {!s}".format(peer, repr(e)))
                self.peers.discard(peer)
            else:
                logger.info("Fallback peer error")
            raise
