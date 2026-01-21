# src/network/server.py
#
# Networking "server" for a node: accepts incoming connections and
# delivers decoded JSON messages to the node’s message handler.
#
# Milestone 1:
#   - start an asyncio server
#   - read one JSON object per line using recv_json
#   - invoke on_message for every decoded message
#
# Milestone 2:
#   - durable loop for each connection
#   - proper cleanup when peers disconnect or errors occur
#
# Milestone 3:
#   - fully supports Paxos traffic (Prepare, Promise, Accept, Accepted, Decide)
#   - server is the entry point for all inbound Paxos and block-related messages
#
# Final updates:
#   - works together with send_json_with_delay on the client side
#   - provides the stable inbound path used by tentative and decided block flow
#   - prints connection lifecycle events for debugging and grading clarity

import asyncio
from typing import Awaitable, Callable, Dict, Any

from .protocol import recv_json

OnMessageCallback = Callable[[Dict[str, Any]], Awaitable[None]]

async def start_server(
    host: str,
    port: int,
    on_message: OnMessageCallback,
) -> asyncio.AbstractServer:
    async def handle_conn(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        print(f"[NET-SERVER] Connection from {peer}", flush=True)

        try:
            while True:
                msg = await recv_json(reader)
                await on_message(msg)
        except ConnectionError:
            pass
        except Exception as e:
            print(f"[NET-SERVER] Error handling connection from {peer}: {e}", flush=True)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            print(f"[NET-SERVER] Connection closed from {peer}", flush=True)

    server = await asyncio.start_server(handle_conn, host, port)
    print(f"[NET-SERVER] Listening on {host}:{port}", flush=True)
    return server
