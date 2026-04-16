from kadena_block_relayer_lib import BlockRelayer
import logging
import asyncio
import uvicorn
import argparse
from proxy_app import get_app


import logging.config

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "[%(levelname)s] %(name)s: %(message)s",
        },
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        },
    },
    "root": {
        "handlers": ["default"],
        "level": "INFO",
    },
    "loggers": {
        "uvicorn": {"level": "INFO"},
        "uvicorn.error": {"level": "INFO"},
        "uvicorn.access": {"level": "INFO"},
    },
}


def main():
    parser = argparse.ArgumentParser(prog="kadena_block_relayer_proxy",
                                     description="Connect to Kadena P2P network and provide blocks streaming API")
    parser.add_argument("--network", type=str, default="mainnet01", help="Network (eg: mainnet01, testnet06)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Bind socket to this host")
    parser.add_argument("--port", type=int, default=8080, help="Port")

    args = parser.parse_args()

    logging.config.dictConfig(LOGGING_CONFIG)
    uvicorn.run(get_app(args.network), host="127.0.0.1", port=5000, log_config=LOGGING_CONFIG)


if __name__ == "__main__":
    main()
