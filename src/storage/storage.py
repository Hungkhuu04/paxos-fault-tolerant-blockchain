# src/storage/storage.py
# This file has helper functions to save and load the blockchain
# and account balances using JSON on disk.

import json
from pathlib import Path

from ..blockchain.block import Block
from ..blockchain.chain import Blockchain
from ..accounts.accounts import AccountsTable


def save_blockchain(blockchain, path):
    p = Path(path)
    blocks_data = []
    for b in blockchain.blocks:
        blocks_data.append(b.to_dict())

    text = json.dumps(blocks_data, indent=2)
    p.write_text(text, encoding="utf-8")


def load_blockchain(path):
    p = Path(path)
    chain = Blockchain()

    if not p.exists():
        return chain

    text = p.read_text(encoding="utf-8").strip()
    if text == "":
        return chain

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return chain

    # data should be a list of block dicts
    for d in data:
        block = Block.from_dict(d)
        chain.blocks.append(block)

    return chain


def save_balances(accounts, path):
    p = Path(path)

    if isinstance(accounts, AccountsTable):
        balances = accounts.balances
    else:
        balances = accounts

    text = json.dumps(balances, indent=2)
    p.write_text(text, encoding="utf-8")


def load_balances(path):
    p = Path(path)

    if not p.exists():
        return AccountsTable.fresh()

    text = p.read_text(encoding="utf-8").strip()
    if text == "":
        return AccountsTable.fresh()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return AccountsTable.fresh()

    # data should be a dict: {"P1": 100, ...}
    return AccountsTable(data)


def _load_log(path: Path):
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}

    if not isinstance(data, dict):
        # if file somehow got a list or something else, ignore it
        return {}

    return data


def log_write_tentative(depth: int, block_dict: dict, path):
    p = Path(path)
    log = _load_log(p)

    key = str(depth)
    entry = log.get(key, {})
    # always store the latest block dict
    entry["block"] = block_dict
    # preserve 'decided' if it was already true; otherwise default to False
    entry["decided"] = entry.get("decided", False)

    log[key] = entry
    p.write_text(json.dumps(log, indent=2), encoding="utf-8")


def log_mark_decided(depth: int, path):
    p = Path(path)
    log = _load_log(p)

    key = str(depth)
    if key not in log:
        # nothing logged (e.g., node crashed before accept); just return
        return

    entry = log[key]
    entry["decided"] = True
    log[key] = entry

    p.write_text(json.dumps(log, indent=2), encoding="utf-8")


def load_ledger_log(path):
    p = Path(path)
    if not p.exists():
        return {}

    text = p.read_text(encoding="utf-8").strip()
    if text == "":
        return {}

    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return {}

    result = {}
    for depth_str, entry in raw.items():
        try:
            depth = int(depth_str)
        except ValueError:
            continue

        block_dict = entry.get("block")
        decided = bool(entry.get("decided", False))
        if not isinstance(block_dict, dict):
            continue

        result[depth] = {
            "block": block_dict,
            "decided": decided,
        }

    return result