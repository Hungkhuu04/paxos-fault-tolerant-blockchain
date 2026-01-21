# src/paxos/messages.py
#
# Defines the Paxos message types and the PaxosMessage class used to
# convert Paxos messages to and from dictionaries for JSON transport.
# Follows the standard Paxos structure:
#   - PREPARE, PROMISE, ACCEPT, ACCEPTED, DECIDE
# Uses ballot numbers as (seq_num, proposer_id, depth) so proposals are totally ordered and tied to a specific log slot. 
# Messages may carry prior accepted ballots/values, proposed block values, and
from enum import Enum
Ballot = tuple[int, int, int]


class PaxosMessageType(str, Enum):
    PREPARE = "PREPARE" # Phase 1a
    PROMISE = "PROMISE" # Phase 1b (promise + prior AcceptNum/AcceptVal)
    ACCEPT = "ACCEPT" # Phase 2a
    ACCEPTED = "ACCEPTED"  # Phase 2b
    DECIDE = "DECIDE" # Phase 3 (announce decision)


class PaxosMessage:
    def __init__(
        self,
        msg_type: PaxosMessageType,
        from_id: int,
        ballot: Ballot,
        depth: int,
        value=None,
        accept_num: Ballot | None = None,
        accept_val=None,
        first_uncommitted: int | None = None,
    ):
        self.type = msg_type
        self.from_id = from_id
        self.ballot = ballot
        self.depth = depth
        self.value = value
        self.accept_num = accept_num
        self.accept_val = accept_val
        self.first_uncommitted = first_uncommitted

    def to_dict(self) -> dict:
        d = {
            "type": self.type.value,
            "from_id": self.from_id,
            "ballot": list(self.ballot),
            "depth": self.depth,
        }

        # Only include optional fields if they are set
        if self.value is not None:
            d["value"] = self.value

        if self.accept_num is not None:
            d["accept_num"] = list(self.accept_num)

        if self.accept_val is not None:
            d["accept_val"] = self.accept_val

        if self.first_uncommitted is not None:
            d["first_uncommitted"] = self.first_uncommitted

        return d

    @staticmethod
    def from_dict(d: dict) -> "PaxosMessage":
        msg_type = PaxosMessageType(d["type"])
        ballot = tuple(d["ballot"])

        accept_num = None
        if "accept_num" in d and d["accept_num"] is not None:
            accept_num = tuple(d["accept_num"])

        first_uncommitted = d.get("first_uncommitted")

        return PaxosMessage(
            msg_type=msg_type,
            from_id=int(d["from_id"]),
            ballot=ballot,
            depth=int(d["depth"]),
            value=d.get("value"),
            accept_num=accept_num,
            accept_val=d.get("accept_val"),
            first_uncommitted=first_uncommitted,
        )
