# src/blockchain/block.py

# Defines the Block class used in our blockchain.
# A block represents one transaction at a specific depth. 
# Defines the proof of work attributes. 
# Blocks are serialized and then deserialized using dicts so they can be persisted as JSON.

class Block:
    def __init__(self, depth, tx, nonce, prev_hash, hash_value):
        self.depth = depth
        self.tx = tx
        self.nonce = nonce
        self.prev_hash = prev_hash
        self.hash = hash_value

    # Convert a Block into a plain dict so it can be written as JSON.
    def to_dict(self):
        sender, receiver, amount = self.tx
        return {
            "depth": self.depth,
            "tx": [sender, receiver, amount],  # stored as a list in JSON
            "nonce": self.nonce,
            "prev_hash": self.prev_hash,
            "hash": self.hash,
        }

    @staticmethod
    # Convert a a plain dict back into a block. Essentially build back the block
    def from_dict(d):
        depth = int(d["depth"])
        sender, receiver, amount = d["tx"]
        tx = (sender, receiver, amount)
        nonce = d["nonce"]
        prev_hash = d["prev_hash"]
        hash_value = d["hash"]
        return Block(depth, tx, nonce, prev_hash, hash_value)
