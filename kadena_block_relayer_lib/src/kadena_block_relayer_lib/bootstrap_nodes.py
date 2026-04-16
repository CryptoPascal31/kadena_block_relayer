from .chainweb_objects import Peer, PeerAddress

BOOTSTRAPS = {
    "mainnet01": [
        Peer(id=None, address=PeerAddress("pl-1.chainweb-community.org", 443)),
        Peer(id=None, address=PeerAddress("pl-2.chainweb-community.org", 443)),
        Peer(id=None, address=PeerAddress("ca-1.chainweb-community.org", 443)),
        Peer(id=None, address=PeerAddress("fr-1.chainweb-community.org", 443)),
        Peer(id=None, address=PeerAddress("nl-1.chainweb-community.org", 443)),
    ],
    "testnet06": [
        Peer(id=None, address=PeerAddress("testnet06-1.chainweb-community.org", 443))
    ],
}
