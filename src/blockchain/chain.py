# src/blockchain/chain.py

# Defines the Blockchain class, which tracks the ordered list of decided blocks.
# Each new block is created from a transaction using the proof of work function,
# Fixed tentative blocks become permanent only once Paxos delivers a DECIDE message.
# The chain stores blocks by depth and verifies depth/prev_hash before appending.


import hashlib
from .block import Block
from .pow import compute_pow

GENESIS_PREV_HASH = "0" * 64   # First block uses dummy prev hash (64 zeros)
class Blockchain:
    def __init__(self):
        self.blocks = []  # list of Block objects

    def length(self):
        return len(self.blocks)

    def last_hash(self) -> str:
        if len(self.blocks) == 0:
            return GENESIS_PREV_HASH # For an empty chain, return the GENESIS_PREV_HASH.
        return self.blocks[-1].hash  # Return the PoW hash of the latest block.

    def new_block_for_tx(self, tx):
        depth = self.length()
        prev_hash = self.last_hash()
        nonce, block_hash = compute_pow(tx)

        block = Block(depth, tx, nonce, prev_hash, block_hash)
        return block

    def append_block(self, block):
        if block.depth != self.length():
            raise ValueError("Block depth mismatch.")

        if block.prev_hash != self.last_hash():
            raise ValueError("prev_hash mismatch.")

        self.blocks.append(block)
