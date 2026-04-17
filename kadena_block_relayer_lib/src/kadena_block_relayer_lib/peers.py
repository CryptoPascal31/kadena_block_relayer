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
                logger.info("Peer DB saved with {:d} peers".format(len(self)))
        except Exception:
            pass

    def load(self):
        try:
            logger.info("Loading peer DB")
            with open(self.db_file, "rb") as f:
                self.peers = pickle.load(f)
            self.queue.clear()
            self.queue.extend(self.randomized_peers)
        except Exception as e:
            logger.warn("Error when loading peer DB: {!s}".format(repr(e)))
            self.peers = set()
        finally:
            logger.info("{:d} peers".format(len(self)))

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
    def randomized_peers(self):
        return random.sample(list(self.peers), len(self))

    @property
    def fallback_peer(self):
        return random.choice(list(self.peers) if self.peers else self.bootstraps)

    @contextmanager
    def next_peer(self):
        try:
            peer = self.queue.popleft()
            from_queue = True
        except IndexError:
            peer = self.fallback_peer
            from_queue = False
            logger.info("No peer available: use a fallback")

        def __enqueue():
            if from_queue:
                self.queue.append(peer)

        try:
            yield peer
            __enqueue()
        except Missingdata:
            # In case it's a mssing data error, it's not fatal, just put back in queue
            __enqueue()
            raise
        except Exception as e:
            if from_queue:
                logger.info("Removing peer {!s} because of: {!s}".format(peer, repr(e)))
                self.peers.discard(peer)
            else:
                logger.info("Fallback Peer {!s} error: {!s}".format(peer, repr(e)))
            raise
