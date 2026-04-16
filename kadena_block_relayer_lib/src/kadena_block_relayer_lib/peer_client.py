import logging

from tenacity import retry, stop_after_attempt, wait_random
from yarl import URL
import msgspec
from msgspec import Struct

from .chainweb_objects import Cut, Peer, PayloadWithOutput, PeerInfos, HeadersInfo
from .kadena_common import b64_decode
from .exceptions import Missingdata

logger = logging.getLogger(__name__)

def ensure_list(x):
    if isinstance(x, list):
        return x
    if x is None:
        return []

    return [x]


async def raise_missing(resp):
    if not resp.ok:
        if resp.status == 404:
            raise Missingdata("")
        resp.raise_for_status()


class PeerClient:
    def __init__(self, network, peerDb, session):
        self.session = session
        self.peerDb = peerDb
        self.network = network

    def peer_to_url(self, peer):
        return URL.build(scheme="https", host=peer.address.hostname, port=peer.address.port)

    def chainweb_url(self, peer, *path):
        return self.peer_to_url(peer).joinpath("chainweb", "0.0", self.network, *path)

    def get(self, peer, *path, raise_for_status=True, params=None):
        url = self.chainweb_url(peer, *path)
        return self.session.get(url, raise_for_status=raise_for_status, ssl=False, params=params)

    def post(self, peer, *path, raise_for_status=True, params=None, data=None):
        url = self.chainweb_url(peer, *path)
        return self.session.post(url, raise_for_status=raise_for_status, ssl=False, params=params, json=data)

    async def getCutFromPeer(self, peer):
        async with self.get(peer, "cut") as resp:
            data = await resp.read()
            return msgspec.json.decode(data, type=Cut)

    async def _getCut(self):
        with self.peerDb.next_peer() as peer:
            return await self.getCutFromPeer(peer)

    async def _getHeaders(self, chain, limit, _next, minheight, maxheight):
        prms = {"limit": str(limit), "minheight": str(minheight), "maxheight": str(maxheight)}
        if _next:
            prms["next"] = _next

        with self.peerDb.next_peer() as peer:
            async with self.get(peer, "chain", chain, "header", params=prms) as resp:
                data = await resp.read()
                return msgspec.json.decode(data, type=HeadersInfo)

    async def _getHeadersBranch(self, chain, limit, _next, minheight, maxheight, lower, upper):
        prms = {"limit": str(limit), "minheight": str(minheight), "maxheight": str(maxheight)}
        data = {"lower": ensure_list(lower), "upper": ensure_list(upper)}
        if _next:
            prms["next"] = _next

        with self.peerDb.next_peer() as peer:
            async with self.post(peer, "chain", chain, "header", "branch", params=prms, data=data) as resp:
                data = await resp.read()
                return msgspec.json.decode(data, type=HeadersInfo)

    async def _getHeader(self, chain, block_hash):
        with self.peerDb.next_peer() as peer:
            async with self.get(peer, "chain", chain, "header", block_hash, raise_for_status=raise_missing) as resp:
                data = await resp.read()
                return b64_decode(data)

    async def _getPayload(self, chain, payload_hash):
        with self.peerDb.next_peer() as peer:
            async with self.get(peer, "chain", chain, "payload", payload_hash, "outputs", raise_for_status=raise_missing) as resp:
                data = await resp.read()
                return msgspec.json.decode(data, type=PayloadWithOutput)

    @retry(stop=stop_after_attempt(32), reraise=True, wait=wait_random(min=0.1, max=0.2))
    async def getHeader(self, chain, block_hash):
        return await self._getHeader(chain, block_hash)

    @retry(stop=stop_after_attempt(32), reraise=True, wait=wait_random(min=0.1, max=0.2))
    async def getHeaders(self, chain, limit, _next, minheight, maxheight):
        return await self._getHeaders(chain, limit, _next, minheight, maxheight)

    @retry(stop=stop_after_attempt(32), reraise=True, wait=wait_random(min=0.1, max=0.2))
    async def getHeadersBranch(self, chain, limit, _next, minheight, maxheight, lower, upper):
        return await self._getHeadersBranch(chain, limit, _next, minheight, maxheight, lower, upper)

    @retry(stop=stop_after_attempt(32), reraise=True, wait=wait_random(min=0.1, max=0.2))
    async def getPayload(self, chain, payload_hash):
        return await self._getPayload(chain, payload_hash)

    @retry(stop=stop_after_attempt(32), reraise=True, wait=wait_random(min=0.1, max=0.2))
    async def getCut(self):
        return await self._getCut()

    async def getPeers(self):
        with self.peerDb.next_peer() as peer:
            logger.info("Getting peers from {!s}".format(peer.address))
            async with self.get(peer, "cut/peer", params={"limit": 32}) as resp:
                data = await resp.read()
                infos = msgspec.json.decode(data, type=PeerInfos)
                return infos.items
