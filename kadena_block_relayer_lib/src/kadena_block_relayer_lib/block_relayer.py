from collections import OrderedDict
import asyncio
from contextlib import asynccontextmanager
import logging

import aiohttp

from .kadena_common import b64_decode
from .chainweb_objects import BlockHeader, BlockEvent, HeaderEvent, BlockWithPayloadOutputs, BlocksInfo
from .peers import PeersDb
from .peer_client import PeerClient


logger = logging.getLogger(__name__)


@asynccontextmanager
async def sleep_after(delay: float):
    cancelled = False
    try:
        yield
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        if not cancelled:
            await asyncio.sleep(delay)


class BoundedSet:
    """An implementation of a bounded Set"""

    def __init__(self, max_elements):
        self.max_elements = max_elements
        self.inner = OrderedDict()

    def __contains__(self, key):
        return key in self.inner

    def add(self, key):
        self.inner[key] = None
        if len(self.inner) > self.max_elements:
            self.inner.popitem(last=False)


class BoundedQueue(asyncio.Queue):
    """An implementation of an async queue, that throw old elements when full"""

    def __init__(self):
        super().__init__(maxsize=30)

    def put_nowait(self, element):
        if self.full():
            self.get_nowait()
        super().put_nowait(element)


class BlockRelayer:
    def __init__(self, network="mainnet01", cut_poll_delay=5.0):
        self.best_cut = None
        self.first_cut_event = asyncio.Event()
        self.handled = BoundedSet(256)
        self.client = None
        self.tasks = None
        self.db = PeersDb(network)
        self.payload_queues = []
        self.headers_queues = []
        self.network = network
        self._cut_poll_delay = cut_poll_delay

    @asynccontextmanager
    async def run(self):
        timeout = aiohttp.ClientTimeout(connect=2, sock_read=2)
        connector = aiohttp.TCPConnector(limit=200, limit_per_host=10, ttl_dns_cache=7200, enable_cleanup_closed=True, keepalive_timeout=7200)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with asyncio.TaskGroup() as self.tasks:
                try:
                    self.client = PeerClient(self.network, self.db, session)
                    tsk_peers = self.tasks.create_task(self._refreshPeersTaks())
                    tsk_cuts = self.tasks.create_task(self._fetchCutTask())
                    yield self

                finally:
                    tsk_peers.cancel()
                    tsk_cuts.cancel()
                    self.db.save()

    async def payload_stream(self):
        try:
            q = BoundedQueue()
            self.payload_queues.append(q)
            while True:
                data = await q.get()
                yield data
        finally:
            self.payload_queues.remove(q)

    async def header_stream(self):
        try:
            q = BoundedQueue()
            self.headers_queues.append(q)
            while True:
                data = await q.get()
                yield data
        finally:
            self.headers_queues.remove(q)

    async def blocks_range(self, chain, minheight, maxheight, reverse=False):
        """ A convenience wrapper of get_block_branches"""
        cut = await self.get_cut()
        tip = cut.hashes[str(chain)].hash

        if reverse:
            _range = range(maxheight-19, minheight-20, -20)
        else:
            _range = range(minheight, maxheight+1, 20)

        for _minheight in _range:
            _minheight = max(_minheight, minheight)
            _maxheight = min(_minheight + 19, maxheight)

            info = await self.get_block_branches(chain, 20, "", _minheight, _maxheight, [], [tip])
            for block in sorted(info.items, reverse=reverse):
                yield block

    def get_block(self, chain, limit, _next, minheight, maxheight):
        return self._handle_headers(chain, self.client.getHeaders(chain, limit, _next, minheight, maxheight))

    def get_block_branches(self, chain, limit, _next, minheight, maxheight, lower, upper):
        return self._handle_headers(chain, self.client.getHeadersBranch(chain, limit, _next, minheight, maxheight, lower, upper))

    async def get_cut(self):
        await self.first_cut_event.wait()
        return self.best_cut

    async def _handle_headers(self, chain, getter):
        resp = await getter
        headers = [BlockHeader.parse(x) for x in map(b64_decode, resp.items)]
        payloads = await asyncio.gather(*map(lambda h: self.client.getPayload(chain, h.payloadHash), headers))
        blocks = map(lambda x, y: BlockWithPayloadOutputs(header=x, payloadWithOutputs=y), headers, payloads)
        return BlocksInfo(items=list(blocks), limit=resp.limit, next=resp.next)

    def _publish_cut(self, new_cut):
        if self.best_cut is None or new_cut > self.best_cut:
            logger.info("New cut received with weight:{:s}".format(new_cut.weight))

            self.best_cut = new_cut
            self.first_cut_event.set()

            self._fetch_missing_blocks()

    async def _fetchCutTask(self):
        while True:
            async with sleep_after(self._cut_poll_delay):
                logger.debug("Getting new cut")
                cut = await self.client.getCut()
                self._publish_cut(cut)

    async def _refreshPeersTaks(self):
        while True:
            async with sleep_after(60):
                try:
                    new_peers = await self.client.getPeers()
                except Exception as e:
                    logger.warn("Error when retrieving peers:{!s}".format(e))
                    continue

                for p in new_peers:
                    if p not in self.db:
                        # Validate the peer
                        try:
                            logger.debug("Trying peer {!s}".format(p.address))
                            await self.client.getCutFromPeer(p)
                            self.db.insert(p)
                        except Exception as e:
                            logger.warn("Peer {!s} Invalid because of {!s}".format(p.address, repr(e)))

    async def _fetchBlockTask(self, chain, block_hash, parents_count=2):
        try:
            raw_header = await self.client.getHeader(chain, block_hash)
            bh = BlockHeader.parse(raw_header)

            # Check in case parents are missing, this can happen with a short time between 2 blocks
            #  => retrieve them first
            if parents_count > 0 and bh.parent not in self.handled:
                logger.info("Parent {:s} of block {:s} missing".format(bh.parent, block_hash))
                self.handled.add(bh.parent)
                await self._fetchBlockTask(chain, bh.parent, parents_count-1)

            payload = await self.client.getPayload(chain, bh.payloadHash)

            if self.payload_queues:
                event = BlockEvent.build(bh, payload)
                for q in self.payload_queues:
                    q.put_nowait(event)

            if self.headers_queues:
                event = HeaderEvent.build(bh, payload)
                for q in self.headers_queues:
                    q.put_nowait(event)

        except Exception as e:
            print(str(e))
            logger.warn("Error when fetching payload")

    def _fetch_missing_blocks(self):
        for chain, val in self.best_cut.hashes.items():
            if val.hash not in self.handled:
                self.handled.add(val.hash)
                self.tasks.create_task(self._fetchBlockTask(chain, val.hash))
