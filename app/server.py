"""Run telemetry only while this Uvicorn process owns a bound listener."""
from __future__ import annotations

import argparse
import asyncio
import os
import threading
import uuid

import uvicorn

from app.main import PROJECT_ID, VERSION
from xradar import Capabilities, config_from_env, spawn_capabilities_heartbeat


class Server(uvicorn.Server):
    heartbeat_interval = 900

    def __init__(self, config):
        super().__init__(config)
        self.heartbeat_stop = threading.Event()
        self.heartbeat_task = None

    async def startup(self, sockets=None):
        await super().startup(sockets)
        config = config_from_env()
        if not self.started or config is None:
            return
        caps = (
            Capabilities.host_introspect(os.environ.get("W_RADAR_PROJECT_ID") or PROJECT_ID)
            .with_instance_id(os.environ.get("W_RADAR_INSTANCE_ID") or f"{PROJECT_ID}-{uuid.uuid4()}")
            .with_sdk("w-papers-doi-catalog", VERSION)
            .with_git_from_env()
        )
        self.heartbeat_task = spawn_capabilities_heartbeat(
            config, caps, self.heartbeat_interval, self.heartbeat_stop
        )

    async def shutdown(self, sockets=None):
        self.heartbeat_stop.set()
        if self.heartbeat_task is not None:
            await asyncio.wrap_future(self.heartbeat_task)
        await super().shutdown(sockets)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("BIND_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("BIND_PORT", "55221")))
    parser.add_argument("--no-proxy-headers", action="store_true")
    args = parser.parse_args()
    Server(uvicorn.Config("app.main:app", host=args.host, port=args.port, proxy_headers=False)).run()


if __name__ == "__main__":
    main()
