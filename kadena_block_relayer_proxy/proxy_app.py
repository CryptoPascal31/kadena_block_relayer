from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route
from starlette.exceptions import HTTPException

from kadena_block_relayer_lib import BlockRelayer

import msgspec
from msgspec import Struct

API_VERSION = "0.0"
VERSION = "1.0"
BLOCKDELAY = 30e6
CHAINS = 20

async def async_map(func, async_iterable):
    async for item in async_iterable:
        yield func(item)


@asynccontextmanager
async def lifespan(app):
    relayer = BlockRelayer(app.state.NETWORK, cut_poll_delay=5.0)
    async with relayer.run():
        app.state["relayer"] = relayer
        yield


class Info(Struct, frozen=True):
    nodeApiVersion: str
    nodeBlockDelay: int
    nodeChains: [str]
    nodeNumberOfChains: int
    nodePackageVersion: str
    nodeVersion: str

class LowerUpper(Struct, frozen=True):
    lower: list[str]
    upper: list[str]


class JsonMsgspecRsponse(Response):
    media_type = "application/json"
    charset = "ascii"
    encoder = msgspec.json.Encoder()

    def render(self, content):
        return self.encoder.encode(content)


class MsgPackMsgspecRsponse(Response):
    media_type = "application/vnd.msgpack"
    charset = "ascii"
    encoder = msgspec.msgpack.Encoder()

    def render(self, content):
        return self.encoder.encode(content)


def reponse_class(request):
    if "application/vnd.msgpack" in request.headers.get("Accept", ""):
        return MsgPackMsgspecRsponse
    return JsonMsgspecRsponse


def get_paging_params(request):
    limit = request.query_params.get("limit", None)
    _next = request.query_params.get("next", None)

    minheight = request.query_params.get("minheight", None)
    maxheight = request.query_params.get("maxheight", None)
    chain = request.path_params["chain"]

    if limit is None or minheight is None or maxheight is None:
        raise HTTPException(status_code=400, detail="Bad request")

    if not 0 <= chain < CHAINS:
        raise HTTPException(status_code=400, detail="Bad request")

    return (str(chain), int(limit), _next, minheight, maxheight)


def info(request):
    resp_cls = reponse_class(request)
    return resp_cls(request.app.info)


async def cut(request):
    resp_cls = reponse_class(request)
    return resp_cls(await request.app.state["relayer"].get_cut())


async def block(request):
    params = get_paging_params(request)
    resp_cls = reponse_class(request)
    data = await request.app.state["relayer"].get_block(*params)
    resp_cls = reponse_class(request)
    return resp_cls(data)


async def blockBranch(request):
    (chain, limit, _next, minheight, maxheight)= get_paging_params(request)
    resp_cls = reponse_class(request)
    incoming_data = msgspec.json.decode(await request.body(), type=LowerUpper)

    data = await request.app.state["relayer"].get_block_branches(chain, limit, _next, minheight, maxheight, incoming_data.lower, incoming_data.upper)
    resp_cls = reponse_class(request)
    return resp_cls(data)


def serialize_event(ev):
    b = bytearray()
    b.extend(b"event:")
    b.extend(ev.eventType)
    b.extend(b"\n")
    b.extend(b"data:")
    JsonMsgspecRsponse.encoder.encode_into(ev, b, -1)
    b.extend(b"\n")
    return memoryview(b)


async def block_updates(request):
    stream = request.app.state["relayer"].payload_stream()
    return StreamingResponse(async_map(serialize_event, stream), media_type=JsonMsgspecRsponse.media_type)


async def header_updates(request):
    stream = request.app.state["relayer"].header_stream()
    return StreamingResponse(async_map(serialize_event, stream), media_type=JsonMsgspecRsponse.media_type)


def get_app(network):
    def chainweb_route(x):
        return "/chainweb/" + API_VERSION + "/" + network + x

    routes = [ Route(chainweb_route("/cut"), cut, methods=['GET']),
               Route("/info", info, methods=['GET']),
               Route(chainweb_route("/chain/{chain:int}/block"), block, methods=['GET', 'POST']),
               Route(chainweb_route("/chain/{chain:int}/block/branch"), blockBranch, methods=['POST']),
               Route(chainweb_route("/block/updates"), block_updates, methods=['GET', 'POST']),
               Route(chainweb_route("/header/updates"), header_updates, methods=['GET', 'POST'])]

    app = Starlette(debug=True, routes=routes, lifespan=lifespan)
    app.state.NETWORK = network

    app.info = Info(nodeApiVersion=API_VERSION,
                    nodeBlockDelay=BLOCKDELAY,
                    nodeChains=[str(c) for c in range(CHAINS)],
                    nodeNumberOfChains=CHAINS,
                    nodePackageVersion=VERSION,
                    nodeVersion=network)

    return app
