# src/network/protocol.py
#
# Common networking helpers for all nodes:
#   - JSON line protocol
#   - artificial network delay for realistic message timing
#
# Milestone 1:
#   - basic JSON send/receive utilities
#   - one JSON message per line
#   - minimal helper functions for the network layer
#
# Milestone 2:
#   - added send_json_with_delay so every outbound message can experience NETWORK_DELAY
#
# Milestone 3:
#   - fully integrated into Paxos message passing (Prepare, Promise, Accept, Accepted, Decide)
#   - used uniformly by both client and server networking paths
#
# Final updates:
#   - timing behavior now matches the full system: all Paxos and block messages flow through this JSON protocol with the configured delay
#   - unchanged core design: simple, deterministic, line-based JSON protocol


import asyncio
import json
from typing import Any, Dict

NETWORK_DELAY: float = 3.0

async def send_json(writer: asyncio.StreamWriter, obj: Dict[str, Any]) -> None:
    line = json.dumps(obj) + "\n"
    writer.write(line.encode("utf-8"))
    await writer.drain()


async def recv_json(reader: asyncio.StreamReader) -> Dict[str, Any]:
    line = await reader.readline()
    if not line:
        raise ConnectionError("socket closed")
    return json.loads(line.decode("utf-8"))


async def send_json_with_delay(
    writer: asyncio.StreamWriter, obj: Dict[str, Any]
) -> None:
    await asyncio.sleep(NETWORK_DELAY)
    await send_json(writer, obj)
