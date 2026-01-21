# src/blockchain/pow.py

# This file implements a simple proof-of-work for a transaction.
# We keep trying random nonces until the SHA256 hash ends with 0-4.

import hashlib, os

def tx_to_bytes(tx):
    sender, receiver, amount = tx
    text = f"{sender}:{receiver}:{amount}" # Convert a transaction, so can hash it.
    return text.encode("utf-8")

def compute_pow(tx):
    base = tx_to_bytes(tx) # using the bytes from above

    while True:
        nonce_bytes = os.urandom(16) # pick a random 16-byte nonce
        nonce_hex = nonce_bytes.hex()
        h = hashlib.sha256(base + nonce_bytes).hexdigest() # compute SHA256(tx_bytes + nonce_bytes)

        if h[-1] in "01234":
            return nonce_hex, h # check last hex digit
