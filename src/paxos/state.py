# src/paxos/state.py
#
# Defines the per-node Paxos state and the per-depth (per log slot)
# The Acceptor state used during consensus. Each slot has its own
# AcceptorState tracking:
#   - ballot_num  (highest ballot promised)
#   - accept_num  (highest ballot accepted)
#   - accept_val  (value accepted for that ballot)
#
# PaxosNodeState manages these AcceptorState objects and generates new ballot numbers.
# They should be matching the replicated-log model where each depth runs its own Paxos instance.
from .messages import Ballot

EMPTY_BALLOT: Ballot = (0, 0, 0)

class AcceptorState:
    def __init__(
        self,
        ballot_num: Ballot | None = None,
        accept_num: Ballot | None = None,
        accept_val=None,
    ):
        if ballot_num is None:
            ballot_num = EMPTY_BALLOT
        if accept_num is None:
            accept_num = EMPTY_BALLOT

        self.ballot_num = ballot_num
        self.accept_num = accept_num
        self.accept_val = accept_val


class PaxosNodeState:
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.instances: dict[int, AcceptorState] = {}
        self.next_seq_num = 1  #increments for each new ballot

    def get_acceptor_state(self, depth: int) -> AcceptorState:
        if depth not in self.instances:
            self.instances[depth] = AcceptorState()
        return self.instances[depth]

    def new_ballot(self, depth: int) -> Ballot:
        seq = self.next_seq_num
        self.next_seq_num += 1
        return (seq, self.node_id, depth)
