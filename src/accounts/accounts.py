# src/accounts/accounts.py

# Keeps track of account balances for all known clients.
# Uses CLIENT_IDS and INITIAL_BALANCE to create a new AccountsTable,
# Applies money transfer transactions by debiting the sender and crediting the receiver.

CLIENT_IDS = ["P1", "P2", "P3", "P4", "P5"]
INITIAL_BALANCE = 100  # starting money for each client

class AccountsTable:
    def __init__(self, balances=None):
        if balances is None:
            balances = {}
        self.balances = balances

    @staticmethod
    def fresh():
        balances = {cid: INITIAL_BALANCE for cid in CLIENT_IDS}
        return AccountsTable(balances) # Create a new AccountsTable where every client starts with INITIAL_BALANCE.

    def can_debit(self, client_id, amount):
        current = self.balances.get(client_id, 0)
        return current >= amount

    def apply_transaction(self, tx):
        sender, receiver, amount = tx

        if not self.can_debit(sender, amount):
            print(f"[ACCOUNTS] insufficient funds for {sender} (tried to send {amount})")
            raise ValueError(f"Insufficient funds for {sender}")

        self.balances[sender] -= amount
        if receiver not in self.balances:
            self.balances[receiver] = 0
        self.balances[receiver] += amount
