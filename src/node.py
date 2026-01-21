# src/node.py

import argparse
import json
import sys
import asyncio
from pathlib import Path

from .blockchain.chain import Blockchain
from .accounts.accounts import AccountsTable
from .paxos.state import PaxosNodeState
from .paxos.instance import PaxosInstance
from .paxos.messages import PaxosMessage
from .storage.storage import (
    save_blockchain,
    load_blockchain,
    save_balances,
    load_balances,
    log_write_tentative,
    log_mark_decided,
    load_ledger_log,
)
from .blockchain.block import Block
from .network.server import start_server
from .network.client import NetworkClient
from .cli.commands import run_cli


class NodeConfig:
    def __init__(self, node_id, host, port, data_dir):
        self.id = node_id
        self.host = host
        self.port = port
        self.data_dir = data_dir


class Node:
    def __init__(self, config, peers):
        self.config = config # NodeConfig for this node
        self.peers = peers # list of NodeConfig for other nodes

        #pathing stuff for our directories
        self.config.data_dir.mkdir(parents=True, exist_ok=True)

        # paths for persistence
        self.blockchain_path = self.config.data_dir / "blockchain.json"
        self.balances_path = self.config.data_dir / "balances.json"
        # log for tentative vs decided blocks
        self.log_path = self.config.data_dir / "ledger_log.json"

        # load existing state if present, otherwise start fresh
        self.blockchain: Blockchain = load_blockchain(self.blockchain_path)
        self.accounts: AccountsTable = load_balances(self.balances_path)

        # Paxos node-wide state (BallotNum / AcceptNum / AcceptVal per depth)
        self.paxos_state = PaxosNodeState(self.config.id)
        # depth -> PaxosInstance
        self.paxos_instances: dict[int, PaxosInstance] = {}

        # Perform crash recovery using ledger_log.json
        self._recover_from_ledger()

        # Networking: server + outgoing client
        self.server: asyncio.AbstractServer | None = None
        self.net_client: NetworkClient | None = None

    # Helpers for first_uncommitted_index
    def _compute_first_uncommitted_index(self) -> int:
        return self.blockchain.length()

    def _repair_peer_chain(self, peer_id: int, peer_index: int, local_index: int) -> None:
        if self.net_client is None:
            print(
                f"[NODE {self.config.id}] _repair_peer_chain: net_client not ready",
                flush=True,
            )
            return

        # Normalize
        if peer_index is None:
            return

        peer_index = int(peer_index)
        local_index = int(local_index)

        # Case 1: peer behind -> push blocks
        if peer_index < local_index:
            start_depth = max(peer_index, 0)
            end_depth = local_index

            # Collect blocks [start_depth, end_depth)
            missing_blocks = []
            for depth in range(start_depth, end_depth):
                if 0 <= depth < len(self.blockchain.blocks):
                    missing_blocks.append(self.blockchain.blocks[depth].to_dict())

            if not missing_blocks:
                return

            payload = {
                "repair_type": "REPAIR_BLOCKS",
                "from_id": self.config.id,
                "target_id": peer_id,
                "start_depth": start_depth,
                "end_depth": end_depth,
                "blocks": missing_blocks,
            }

            print(
                f"[NODE {self.config.id}] Sending REPAIR_BLOCKS to node {peer_id} "
                f"for depths [{start_depth}, {end_depth})",
                flush=True,
            )

            async def _send():
                await self.net_client.send_to_peer(peer_id, payload)

            asyncio.create_task(_send())
            return

        # Case 2: we are behind to request repair
        if local_index < peer_index:
            start_depth = max(local_index, 0)
            end_depth = peer_index

            payload = {
                "repair_type": "REPAIR_REQUEST",
                "from_id": self.config.id,
                "target_id": peer_id,
                "start_depth": start_depth,
                "end_depth": end_depth,
            }

            print(
                f"[NODE {self.config.id}] Sending REPAIR_REQUEST to node {peer_id} "
                f"for depths [{start_depth}, {end_depth})",
                flush=True,
            )

            async def _send_req():
                await self.net_client.send_to_peer(peer_id, payload)

            asyncio.create_task(_send_req())
            return

        # Equal indexes to nothing to do.

    # build peer map for NetworkClient
    def _build_peer_map(self) -> dict[int, tuple[str, int]]:
        peers: dict[int, tuple[str, int]] = {}
        peers[self.config.id] = (self.config.host, self.config.port)
        for p in self.peers:
            peers[p.id] = (p.host, p.port)
        return peers


    # Paxos integration
    def _get_paxos_instance(self, depth):
        if depth not in self.paxos_instances:
            peer_ids = [p.id for p in self.peers]
            inst = PaxosInstance(
                depth=depth,
                node_state=self.paxos_state,
                peers=peer_ids,
                send_func=self._send_paxos_to_peer,
                broadcast_func=self._broadcast_paxos,
                on_decide=self._on_paxos_decide,
                on_tentative=self._on_paxos_tentative,
                get_first_uncommitted=self._compute_first_uncommitted_index,
                repair_callback=self._repair_peer_chain,
            )
            self.paxos_instances[depth] = inst
        return self.paxos_instances[depth]

    def start_paxos(self, depth, value):
        inst = self._get_paxos_instance(depth)
        inst.start_proposal(value)

    # Paxos callbacks: tentative + decided
    def _on_paxos_tentative(self, depth, value):
        # Rebuild Block from value
        if isinstance(value, dict):
            try:
                block = Block.from_dict(value)
            except Exception as e:
                print(
                    f"[NODE {self.config.id}] ERROR reconstructing tentative Block from value: {e}",
                    flush=True,
                )
                return
        elif isinstance(value, Block):
            block = value
        else:
            print(
                f"[NODE {self.config.id}] Unexpected tentative value type: {type(value)}",
                flush=True,
            )
            return

        # Write / update tentative entry in the log
        try:
            log_write_tentative(depth, block.to_dict(), self.log_path)
            print(
                f"[NODE {self.config.id}] Logged tentative block at depth={depth}",
                flush=True,
            )
        except Exception as e:
            print(
                f"[NODE {self.config.id}] ERROR writing tentative log at depth={depth}: {e}",
                flush=True,
            )

    def _on_paxos_decide(self, depth, value):
        print(
            f"[NODE {self.config.id}] DECIDED depth={depth}, value={value!r}",
            flush=True,
        )

        #Rebuild Block from value
        if isinstance(value, dict):
            try:
                block = Block.from_dict(value)
            except Exception as e:
                print(
                    f"[NODE {self.config.id}] ERROR reconstructing Block from value: {e}",
                    flush=True,
                )
                return
        elif isinstance(value, Block):
            block = value
        else:
            print(
                f"[NODE {self.config.id}] Unexpected decided value type: {type(value)}",
                flush=True,
            )
            return

        #sanity check: depth agreement
        if block.depth != depth:
            print(
                f"[NODE {self.config.id}] WARNING: decided depth mismatch: "
                f"block.depth={block.depth}, slot={depth}",
                flush=True,
            )

        # Append block to blockchain (this enforces depth/prev_hash checks)
        try:
            self.blockchain.append_block(block)
        except ValueError as e:
            #happens if already appended this block or chain diverged.
            print(
                f"[NODE {self.config.id}] ERROR appending decided block at depth={depth}: {e}",
                flush=True,
            )
            return

        # Apply the transaction to account balances
        sender, receiver, amount = block.tx
        # we assume tx was valid when it was proposed.
        self.accounts.apply_transaction(block.tx)

        # Mark decided in the log (Option 2: tentative to decided)
        try:
            log_mark_decided(depth, self.log_path)
            print(
                f"[NODE {self.config.id}] Marked block at depth={depth} as DECIDED in log",
                flush=True,
            )
        except Exception as e:
            print(
                f"[NODE {self.config.id}] ERROR marking decided in log at depth={depth}: {e}",
                flush=True,
            )

        # Persist new state to disk
        save_blockchain(self.blockchain, self.blockchain_path)
        save_balances(self.accounts, self.balances_path)

        print(
            f"[NODE {self.config.id}] Applied decided block at depth={block.depth}: "
            f"{sender} -> {receiver}, amount={amount}",
            flush=True,
        )


    # Network send helpers for Paxos
    def _send_paxos_to_peer(self, peer_id, msg: PaxosMessage):
        if not isinstance(msg, PaxosMessage):
            print(
                f"[NODE {self.config.id}] _send_paxos_to_peer got non-PaxosMessage: {msg}",
                flush=True,
            )
            return

        if self.net_client is None:
            print(
                f"[NODE {self.config.id}] net_client not ready; dropping send_to_peer({peer_id}, {msg.to_dict()})",
                flush=True,
            )
            return

        async def _run():
            await self.net_client.send_to_peer(peer_id, msg.to_dict())

        asyncio.create_task(_run())

    def _broadcast_paxos(self, msg: PaxosMessage):
        if not isinstance(msg, PaxosMessage):
            print(
                f"[NODE {self.config.id}] _broadcast_paxos got non-PaxosMessage: {msg}",
                flush=True,
            )
            return

        if self.net_client is None:
            print(
                f"[NODE {self.config.id}] net_client not ready; dropping broadcast({msg.to_dict()})",
                flush=True,
            )
            return

        async def _run():
            await self.net_client.broadcast(msg.to_dict())

        asyncio.create_task(_run())

    # Cluster repair message handling
    async def _handle_repair_message(self, d: dict):
        """
          REPAIR_BLOCKS:
            {
              "repair_type": "REPAIR_BLOCKS",
              "from_id": <sender_id>,
              "target_id": <int>,
              "start_depth": <int>,
              "end_depth": <int>,
              "blocks": [block_dicts...]
            }
          REPAIR_REQUEST:
            {
              "repair_type": "REPAIR_REQUEST",
              "from_id": <sender_id>,
              "target_id": <int>,
              "start_depth": <int>,
              "end_depth": <int>
            }
        """
        rtype = d.get("repair_type")
        if rtype not in ("REPAIR_BLOCKS", "REPAIR_REQUEST"):
            print(
                f"[NODE {self.config.id}] Unknown repair_type={rtype}",
                flush=True,
            )
            return

        target_id = d.get("target_id")
        if target_id != self.config.id:
            # Not for us.
            return

        if rtype == "REPAIR_REQUEST":
            # Peer is asking us to send blocks [start_depth, end_depth).
            if self.net_client is None:
                return

            start_depth = int(d.get("start_depth", 0))
            end_depth = int(d.get("end_depth", 0))
            sender_id = int(d.get("from_id"))

            missing_blocks = []
            for depth in range(start_depth, end_depth):
                if 0 <= depth < len(self.blockchain.blocks):
                    missing_blocks.append(self.blockchain.blocks[depth].to_dict())

            if not missing_blocks:
                return

            payload = {
                "repair_type": "REPAIR_BLOCKS",
                "from_id": self.config.id,
                "target_id": sender_id,
                "start_depth": start_depth,
                "end_depth": end_depth,
                "blocks": missing_blocks,
            }

            print(
                f"[NODE {self.config.id}] Responding to REPAIR_REQUEST from node {sender_id} "
                f"with REPAIR_BLOCKS for depths [{start_depth}, {end_depth})",
                flush=True,
            )

            await self.net_client.send_to_peer(sender_id, payload)
            return

        # REPAIR_BLOCKS path
        blocks = d.get("blocks", [])
        if not isinstance(blocks, list):
            return

        print(
            f"[NODE {self.config.id}] Received REPAIR_BLOCKS with {len(blocks)} blocks "
            f"from node {d.get('from_id')}",
            flush=True,
        )

        # Sort by depth for safety.
        try:
            blocks_sorted = sorted(blocks, key=lambda bd: bd.get("depth", 0))
        except Exception:
            blocks_sorted = blocks

        for block_dict in blocks_sorted:
            try:
                block = Block.from_dict(block_dict)
            except Exception as e:
                print(
                    f"[NODE {self.config.id}] ERROR reconstructing repaired block: {e}",
                    flush=True,
                )
                continue

            depth = block.depth

            # If we already have a block at this depth, check consistency.
            if depth < len(self.blockchain.blocks):
                existing = self.blockchain.blocks[depth]
                if existing.hash != block.hash:
                    print(
                        f"[NODE {self.config.id}] WARNING: repair block at depth={depth} "
                        f"hash mismatch; keeping existing",
                        flush=True,
                    )
                continue

            # We only accept appends at current chain length.
            if depth != self.blockchain.length():
                print(
                    f"[NODE {self.config.id}] WARNING: repair block depth={depth} "
                    f"!= current chain length={self.blockchain.length()} – skipping",
                    flush=True,
                )
                continue

            # Append and apply tx
            try:
                self.blockchain.append_block(block)
            except ValueError as e:
                print(
                    f"[NODE {self.config.id}] ERROR appending repaired block at depth={depth}: {e}",
                    flush=True,
                )
                continue

            sender, receiver, amount = block.tx
            try:
                self.accounts.apply_transaction(block.tx)
            except Exception as e:
                print(
                    f"[NODE {self.config.id}] ERROR applying repaired tx at depth={depth}: {e}",
                    flush=True,
                )
                continue

            print(
                f"[NODE {self.config.id}] Applied repaired block at depth={depth}: "
                f"{sender} -> {receiver}, amount={amount}",
                flush=True,
            )

        # Persist repaired state
        save_blockchain(self.blockchain, self.blockchain_path)
        save_balances(self.accounts, self.balances_path)

    async def handle_incoming_paxos_dict(self, d):
        # Handle repair messages first (non-Paxos)
        if "repair_type" in d:
            await self._handle_repair_message(d)
            return

        # Normal Paxos path
        msg = PaxosMessage.from_dict(d)
        inst = self._get_paxos_instance(msg.depth)
        inst.handle_message(msg)


    # Application-level entrypoint: money transfer -> Paxos proposal
    def handle_money_transfer(self, debit_id: str, credit_id: str, amount: int):
        # 1) Check that the debit account can pay
        if not self.accounts.can_debit(debit_id, amount):
            print(
                f"[NODE {self.config.id}] Insufficient funds in {debit_id} "
                f"for amount={amount}",
                flush=True,
            )
            return

        tx = (debit_id, credit_id, amount)
        depth = self.blockchain.length()

        # Build candidate block using local blockchain logic (includes PoW)
        block = self.blockchain.new_block_for_tx(tx)

        print(
            f"[NODE {self.config.id}] Proposing tx={tx} at depth={depth}",
            flush=True,
        )

        #For Paxos, send a JSON-friendly representation --- need it in json to do msg stuff like in prev hws
        self.start_paxos(depth=depth, value=block.to_dict())


    # Printing + local test
    def print_summary(self):
        print("=== Node Startup Summary ===")
        print(f"  Node ID     : {self.config.id}")
        print(f"  Host        : {self.config.host}")
        print(f"  Port        : {self.config.port}")
        print(f"  Data dir    : {self.config.data_dir}")
        print(f"  Peers       : {[p.id for p in self.peers]}")
        print("============================")

    def print_blockchain(self):
        print("\n=== Blockchain ===")
        if len(self.blockchain.blocks) == 0:
            print("(empty)")
            return

        for b in self.blockchain.blocks:
            sender, receiver, amount = b.tx
            print(f"- depth   : {b.depth}")
            print(f"  tx      : {sender} -> {receiver}, amount={amount}")
            print(f"  nonce   : {b.nonce}")
            print(f"  hash    : {b.hash}")
            print(f"  prev    : {b.prev_hash}")
            print("")

    def print_balances(self):
        print("\n=== Balances ===")
        for cid, bal in self.accounts.balances.items():
            print(f"  {cid}: {bal}")

    def run_local_test(self):
        print("\n[Local Test] Creating a block locally...\n")

        tx = ("P1", "P2", 10)

        if not self.accounts.can_debit("P1", 10):
            print("Error: P1 cannot pay 10")
            return

        # create + append block
        block = self.blockchain.new_block_for_tx(tx)
        self.blockchain.append_block(block)

        # apply transaction to account balances
        self.accounts.apply_transaction(tx)

        # save new state to disk
        save_blockchain(self.blockchain, self.blockchain_path)
        save_balances(self.accounts, self.balances_path)

        # show block info
        print(f"New block at depth {block.depth}")
        print(f"  tx     : {block.tx}")
        print(f"  nonce  : {block.nonce}")
        print(f"  hash   : {block.hash}")
        print(f"  prev   : {block.prev_hash}")

        # show balances
        print("\nUpdated balances:")
        for cid, bal in self.accounts.balances.items():
            print(f"  {cid}: {bal}")

    # Milestone 2: async networking entrypoint
    async def run_network(self):
        # Start server for incoming Paxos messages
        self.server = await start_server(
            self.config.host,
            self.config.port,
            self.handle_incoming_paxos_dict,
        )

        # set up NetworkClient and connect to peers
        peer_map = self._build_peer_map()
        self.net_client = NetworkClient(self.config.id, peer_map)
        await self.net_client.connect_peers()

        print(f"[NODE {self.config.id}] Network initialized", flush=True)

        # Start CLI loop in background
        asyncio.create_task(run_cli(self))

        # Keep running forever
        await asyncio.Event().wait()

    # Crash recovery using ledger_log.json (extra credit)
    def _recover_from_ledger(self):
        entries = load_ledger_log(self.log_path)
        if not entries:
            print(f"[NODE {self.config.id}] No ledger_log.json or empty; nothing to recover")
            return

        print(f"[NODE {self.config.id}] Recovering from ledger_log.json with {len(entries)} entries")

        # Repair blockchain/balances from decided entries
        # only add blocks that are missing from the chain.
        for depth in sorted(entries.keys()):
            entry = entries[depth]
            decided = entry.get("decided", False)
            block_dict = entry["block"]

            if not decided:
                continue

            block = Block.from_dict(block_dict)

            # If this depth is already in the blockchain, check and skip
            if block.depth < len(self.blockchain.blocks):
                existing = self.blockchain.blocks[block.depth]
                if existing.hash != block.hash:
                    print(
                        f"[NODE {self.config.id}] WARNING: ledger decided block at depth={block.depth} "
                        f"hash mismatch with blockchain",
                        flush=True,
                    )
                continue

            #expect depth == current length
            if block.depth != self.blockchain.length():
                print(
                    f"[NODE {self.config.id}] WARNING: decided block depth={block.depth} "
                    f"does not match current chain length={self.blockchain.length()} – skipping",
                    flush=True,
                )
                continue

            # Append and apply tx
            try:
                self.blockchain.append_block(block)
            except ValueError as e:
                print(
                    f"[NODE {self.config.id}] ERROR appending recovered decided block at depth={depth}: {e}",
                    flush=True,
                )
                continue

            sender, receiver, amount = block.tx
            try:
                self.accounts.apply_transaction(block.tx)
            except Exception as e:
                print(
                    f"[NODE {self.config.id}] ERROR applying recovered tx at depth={depth}: {e}",
                    flush=True,
                )
                # If balances.json is corrupted vs chain, this might go crazy
                # Keep going for now.
                continue

            print(
                f"[NODE {self.config.id}] Recovered decided block at depth={depth}: "
                f"{sender} -> {receiver}, amount={amount}",
                flush=True,
            )

        # Persist repaired state
        save_blockchain(self.blockchain, self.blockchain_path)
        save_balances(self.accounts, self.balances_path)

        # Seed Paxos acceptor state from BOTH tentative and decided entries.
        # This makes the node remember AcceptVal for this depth.
        for depth in sorted(entries.keys()):
            entry = entries[depth]
            block_dict = entry["block"]

            # Treat the block value the same way Paxos sees it over the network: as a dict
            acc = self.paxos_state.get_acceptor_state(depth)

            # Create a synthetic ballot for this acceptance.
            # We don't know the exact original ballot, but we only need a
            # consistent AcceptNum so that future PROMISEs report the right AcceptVal.
            seq = self.paxos_state.next_seq_num
            self.paxos_state.next_seq_num += 1
            synthetic_ballot = (seq, self.paxos_state.node_id, depth)

            # Ensure BallotNum >= AcceptNum
            if acc.ballot_num < synthetic_ballot:
                acc.ballot_num = synthetic_ballot
            acc.accept_num = synthetic_ballot
            acc.accept_val = block_dict

        print(f"[NODE {self.config.id}] Recovery seeding of Paxos acceptor state completed")


def load_config(config_path, my_id):
    try:
        text = config_path.read_text(encoding="utf-8")
        raw = json.loads(text)
    except FileNotFoundError:
        print(f"ERROR: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    nodes = []
    for entry in raw:
        node_cfg = NodeConfig(
            node_id=int(entry["id"]),
            host=str(entry["host"]),
            port=int(entry["port"]),
            data_dir=Path(entry["data_dir"]),
        )
        nodes.append(node_cfg)

    me = None
    peers = []
    for n in nodes:
        if n.id == my_id:
            me = n
        else:
            peers.append(n)

    if me is None:
        print(f"ERROR: no node with id={my_id} in {config_path}", file=sys.stderr)
        sys.exit(1)

    return me, peers


def parse_args():
    parser = argparse.ArgumentParser(description="CS171 Final Project Node")
    parser.add_argument("--id", type=int, required=True, help="Node ID (1-5)")
    parser.add_argument(
        "--config",
        type=str,
        default="config/nodes.json",
        help="Path to nodes config JSON",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    config_path = Path(args.config)

    me, peers = load_config(config_path, args.id)
    node = Node(me, peers)
    node.print_summary()

    # For Milestone 2+, we run the async networking skeleton instead of
    # the local-only blockchain test.
    asyncio.run(node.run_network())


if __name__ == "__main__":
    main()
