 # Kadena Block Relayer

A high performance, lightweight Python library and Proxy app that mimic "Service API" of a node for receiving blocks and headers.

The library dirrecly inetrracts with the Kadena P2P Swarm. (it does't require a full node)

Following endpoints are implemented:
- /info
- /cut
- /chain/{chain:int}/block
- /chain/{chain:int}/block/branch
- /block/updates
- /header/updates

**Note: Not yet production ready.. Reliability is not guaranteed.**