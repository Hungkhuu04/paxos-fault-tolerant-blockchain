# src/network/client.py
#
# Networking "client" for a node: outgoing connections to peers and delivery of Paxos and blockchain-related messages.
#
# Milestone 1:
#   - basic stub client
#   - connect to each peer via asyncio.open_connection
#   - send JSON messages to a single peer
#   - broadcast JSON messages to all peers
#
# Milestone 2:
#   - all outbound messages use send_json_with_delay so every message experiences NETWORK_DELAY
#
# Milestone 3:
#   - full integration with Paxos (Prepare, Promise, Accept, Accepted, Decide)
#   - reconnection when writers break or are missing
#   - cleanup of failed connections to avoid poisoned writers
#
# Final updates:
#   - supports the finalized system where Paxos proposes blocks (with PoW)
#   - network reliably transports tentative and decided block messages
#   - used by node.py as the communication layer for consensus and block flow

import asyncio
from typing import Dict, Tuple, Any

from .protocol import send_json_with_delay

PeerInfo = Tuple[str, int]

class NetworkClient:
    def __init__(
        self,
        node_id: int,
        peers: Dict[int, PeerInfo],
    ) -> None:
        self.node_id = node_id
        self.peers = peers  # includes all nodes

        self._peer_writers: Dict[int, asyncio.StreamWriter] = {}

    async def _connect_peer(self, peer_id: int) -> asyncio.StreamWriter | None:
        peer_info = self.peers.get(peer_id)
        if peer_info is None:
            print(
                f"[NET {self.node_id}] Unknown peer {peer_id}; cannot connect",
                flush=True,
            )
            return None

        host, port = peer_info
        try:
            reader, writer = await asyncio.open_connection(host, port)
            self._peer_writers[peer_id] = writer
            print(
                f"[NET {self.node_id}] Connected to peer {peer_id} at {host}:{port}",
                flush=True,
            )
            return writer
        except Exception as e:
            print(
                f"[NET {self.node_id}] Failed to connect to peer {peer_id} at {host}:{port}: {e}",
                flush=True,
            )
            return None

    async def connect_peers(self) -> None:
        for pid in self.peers.keys():
            if pid == self.node_id:
                continue
            await self._connect_peer(pid)

    async def send_to_peer(self, peer_id: int, msg: Dict[str, Any]) -> None:
        writer = self._peer_writers.get(peer_id)

        # If we don't have a writer yet, try to connect lazily.
        if writer is None:
            writer = await self._connect_peer(peer_id)
            if writer is None:
                print(
                    f"[NET {self.node_id}] Dropping message to {peer_id} (no connection): {msg}",
                    flush=True,
                )
                return

        try:
            await send_json_with_delay(writer, msg)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            print(
                f"[NET {self.node_id}] Error sending to peer {peer_id}: {e}",
                flush=True,
            )
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self._peer_writers.pop(peer_id, None)
        except Exception as e:
            # Any other random error; log but don't keep a poisoned writer.
            print(
                f"[NET {self.node_id}] Unexpected error sending to peer {peer_id}: {e}",
                flush=True,
            )
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self._peer_writers.pop(peer_id, None)

    async def broadcast(self, msg: Dict[str, Any]) -> None:
        for pid in self.peers.keys():
            if pid == self.node_id:
                continue
            await self.send_to_peer(pid, msg)
