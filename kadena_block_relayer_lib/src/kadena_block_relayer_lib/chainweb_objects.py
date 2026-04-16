import struct
from dataclasses import dataclass
from functools import total_ordering
from typing import Optional, ClassVar

from msgspec import Struct

from .kadena_common import b64_encode, b64_decode, pow_hash_b64


class PeerAddress(Struct, frozen=True, cache_hash=True):
    hostname: str
    port: int

    def __str__(self):
        return "{:s}:{:d}".format(self.hostname, self.port)


class Peer(Struct, frozen=True, cache_hash=True):
    id: str | None
    address: PeerAddress

    def __str__(self):
        return str(self.address)


### Structre of the header
HEAD = struct.Struct("<Q Q 32s H")
ADJACENT = struct.Struct("<I 32s")  # Adjacent repeated N times
TAIL = struct.Struct("<32s 32s I 32s Q I Q Q 32s")

VERSIONS = {5: "mainnet01", 7: "testnet04", 8: "testnet06", 2: "development"}


@dataclass(frozen=True, slots=True, eq=False)
class BlockHeader:
    creationTime: int
    parent: str
    height: int
    hash: str
    chainId: int
    weight: str
    featureFlags: int
    epochStart: int
    adjacents: dict
    payloadHash: str
    chainwebVersion: str
    target: str
    nonce: str
    _powHash: str

    @classmethod
    def parse(cls, raw):
        featureFlags, creationTime, parent, adj_length = HEAD.unpack_from(raw)
        adjacent_section = raw[HEAD.size : HEAD.size + ADJACENT.size * adj_length]

        (target, payloadHash, chainId, weight, height, chainwebVersion, epochStart, nonce, _hash) = TAIL.unpack_from(raw, offset=HEAD.size + adj_length * ADJACENT.size)

        adjacents = { c: b64_encode(h) for (c, h) in struct.iter_unpack("<I 32s", adjacent_section)}
        powHash = pow_hash_b64(raw[:-32])

        return cls(creationTime=creationTime,
                   parent=b64_encode(parent),
                   height=height,
                   hash=b64_encode(_hash),
                   chainId=chainId,
                   weight=b64_encode(weight),
                   featureFlags=featureFlags,
                   epochStart=epochStart,
                   adjacents=adjacents,
                   payloadHash=b64_encode(payloadHash),
                   chainwebVersion=VERSIONS.get(chainwebVersion, "unknown"),
                   target=b64_encode(target),
                   nonce=str(nonce),
                   _powHash=powHash)

    def __eq__(self, other):
        return self.hash == other.hash

    def __hash__(self):
        return hash(self.hash)


class PayloadWithOutput(Struct, frozen=True):
    transactions: list[list[str]]
    minerData: str
    transactionsHash: str
    outputsHash: str
    payloadHash: str
    coinbase: str


class BlockEvent(Struct, frozen=True):
    eventType: ClassVar[bytes] = b"Block"
    txCount: int
    powHash: str
    header: BlockHeader
    payloadWithOutputs: PayloadWithOutput
    target: str

    @classmethod
    def build(cls, header, payload):
        return cls(txCount=len(payload.transactions),
                   powHash=header._powHash,
                   header=header,
                   payloadWithOutputs=payload,
                   target=header.target)

class HeaderEvent(Struct, frozen=True):
    eventType: ClassVar[bytes] = b"HeaderBlock"
    txCount: int
    powHash: str
    header: BlockHeader
    target: str

    @classmethod
    def build(cls, header, payload):
        return cls(txCount=len(payload.transactions),
                   powHash=header._powHash,
                   header=header,
                   target=header.target)


@dataclass(frozen=True, slots=True)
class BlockWithPayloadOutputs:
    header: BlockHeader
    payloadWithOutputs: PayloadWithOutput


class CutHash(Struct, frozen=True):
    height: int
    hash: str


@total_ordering
class Cut(Struct, frozen=True):
    instance: str
    id: str
    hashes: dict[str, CutHash]
    height: int
    weight: str
    origin: Optional[Peer] = None

    @property
    def decoded_weight(self):
        return int.from_bytes(b64_decode(self.weight), "little")

    def __eq__(self, other):
        self.id = other.id

    def __gt__(self, other):
        return self.decoded_weight > other.decoded_weight
