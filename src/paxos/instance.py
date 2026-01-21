# src/paxos/instance.py
#
# This file defines PaxosInstance, which manages one Paxos consensus
# instance for a single depth (log index / block depth).
#
# Phases:
#   - Phase 1: PREPARE / PROMISE
#   - Phase 2: ACCEPT / ACCEPTED
#   - Phase 3: DECIDE

from .messages import PaxosMessage, PaxosMessageType, Ballot
from .state import PaxosNodeState

class PaxosInstance:
    def __init__(
        self,
        depth: int,
        node_state: PaxosNodeState,
        peers,
        send_func,
        broadcast_func,
        on_decide,
        on_tentative=None,
        get_first_uncommitted=None,
        repair_callback=None,
    ):
        self.depth = depth
        self.node_state = node_state
        self.peers = peers
        self.send_func = send_func
        self.broadcast_func = broadcast_func
        self.on_decide = on_decide
        self.on_tentative = on_tentative
        self.get_first_uncommitted = get_first_uncommitted
        self.repair_callback = repair_callback

        # proposer side fields
        self.proposal_value = None
        self.proposal_ballot: Ballot | None = None

        # track responses from peers
        self.promises_received: dict[int, PaxosMessage] = {}
        self.accepted_received: dict[int, PaxosMessage] = {}

        # decision tracking
        self.decided_value = None
        self.is_decided = False

    def _quorum_size(self) -> int:
        total = len(self.peers) + 1
        return total // 2 + 1

    def _current_first_uncommitted(self) -> int | None:
        if self.get_first_uncommitted is None:
            return None
        return self.get_first_uncommitted()
    
    def start_proposal(self, value):
        if self.is_decided:
            print(f"[PAXOS depth={self.depth}] already decided, ignoring new proposal")
            return

        #choose a fresh ballot and store the value we want to propose.
        self.proposal_value = value
        self.proposal_ballot = self.node_state.new_ballot(self.depth)

        #clear any old responses from previous rounds.
        self.promises_received.clear()
        self.accepted_received.clear()

        print(
            f"[PAXOS depth={self.depth}] start_proposal: ballot={self.proposal_ballot}, "
            f"value={value!r}",
            flush=True,
        )

        acc = self.node_state.get_acceptor_state(self.depth)
        #only promise if this ballot is >= our current BallotNum.
        if self.proposal_ballot >= acc.ballot_num:
            acc.ballot_num = self.proposal_ballot
            local_promise = PaxosMessage(
                msg_type=PaxosMessageType.PROMISE,
                from_id=self.node_state.node_id,
                ballot=self.proposal_ballot,
                depth=self.depth,
                accept_num=acc.accept_num,
                accept_val=acc.accept_val,
                first_uncommitted=self._current_first_uncommitted(),
            )
            # Directly handle our own PROMISE
            self._on_promise(local_promise)
        else:
            # If we can't even promise to ourselves, its cooked
            print(
                f"[PAXOS depth={self.depth}] local acceptor refused ballot {self.proposal_ballot}, "
                f"current BallotNum={acc.ballot_num}",
                flush=True,
            )
            return

        # Send PREPARE to all peers.
        prepare_msg = PaxosMessage(
            msg_type=PaxosMessageType.PREPARE,
            from_id=self.node_state.node_id,
            ballot=self.proposal_ballot,
            depth=self.depth,
            first_uncommitted=self._current_first_uncommitted(),
        )
        self.broadcast_func(prepare_msg)

    def handle_message(self, msg: PaxosMessage):
        """
        Called by Node when a Paxos message for this depth arrives.
        """
        if msg.type == PaxosMessageType.PREPARE:
            self._on_prepare(msg)
        elif msg.type == PaxosMessageType.PROMISE:
            self._on_promise(msg)
        elif msg.type == PaxosMessageType.ACCEPT:
            self._on_accept(msg)
        elif msg.type == PaxosMessageType.ACCEPTED:
            self._on_accepted(msg)
        elif msg.type == PaxosMessageType.DECIDE:
            self._on_decide(msg)
        else:
            print(f"[PAXOS depth={self.depth}] unknown message type: {msg.type}")

    def _on_prepare(self, msg: PaxosMessage):
        acc = self.node_state.get_acceptor_state(self.depth)

        if msg.ballot < acc.ballot_num:
            #Ignore smaller ballots.
            print(
                f"[PAXOS depth={self.depth}] PREPARE from {msg.from_id} with ballot={msg.ballot} "
                f"rejected (BallotNum={acc.ballot_num})",
                flush=True,
            )
            return

        #promise to not accept proposals with smaller ballot.
        acc.ballot_num = msg.ballot
        promise = PaxosMessage(
            msg_type=PaxosMessageType.PROMISE,
            from_id=self.node_state.node_id,
            ballot=msg.ballot,
            depth=self.depth,
            accept_num=acc.accept_num,
            accept_val=acc.accept_val,
            first_uncommitted=self._current_first_uncommitted(),
        )

        print(
            f"[PAXOS depth={self.depth}] PREPARE from {msg.from_id} accepted, "
            f"sending PROMISE with accept_num={acc.accept_num}, accept_val={acc.accept_val!r}",
            flush=True,
        )

        # Send PROMISE back to proposer.
        self.send_func(msg.from_id, promise)

    def _on_promise(self, msg: PaxosMessage):
        if self.is_decided:
            return

        if self.proposal_ballot is None:
            # We're not currently proposing anything.
            print(
                f"[PAXOS depth={self.depth}] PROMISE from {msg.from_id} ignored: "
                f"no active proposal",
                flush=True,
            )
            return

        if msg.ballot != self.proposal_ballot:
            #old round or different ballot; ignore.
            print(
                f"[PAXOS depth={self.depth}] PROMISE from {msg.from_id} ignored: "
                f"ballot mismatch (msg={msg.ballot}, ours={self.proposal_ballot})",
                flush=True,
            )
            return

        if msg.from_id in self.promises_received:
            # Duplicate PROMISE; ignore.
            return

        self.promises_received[msg.from_id] = msg

        print(
            f"[PAXOS depth={self.depth}] PROMISE from {msg.from_id} "
            f"(total={len(self.promises_received)}/{self._quorum_size()})",
            flush=True,
        )

        # Check if we have a quorum of PROMISEs.
        if len(self.promises_received) < self._quorum_size():
            return

        # We have a majority of PROMISEs. Choose the value to propose in ACCEPT.
        # If any PROMISE reports a prior accepted value, pick the one with
        # highest AcceptNum. Otherwise, keep our original proposal_value just like in slides.
        chosen_value = self.proposal_value
        highest_accept_num: Ballot | None = None

        for p in self.promises_received.values():
            if p.accept_val is not None and p.accept_num is not None:
                if highest_accept_num is None or p.accept_num > highest_accept_num:
                    highest_accept_num = p.accept_num
                    chosen_value = p.accept_val

        self.proposal_value = chosen_value

        print(
            f"[PAXOS depth={self.depth}] PROMISE quorum reached, "
            f"chosen_value={chosen_value!r}, highest_accept_num={highest_accept_num}",
            flush=True,
        )

        # Send ACCEPT(ballot, chosen_value) to all peers.
        accept_msg = PaxosMessage(
            msg_type=PaxosMessageType.ACCEPT,
            from_id=self.node_state.node_id,
            ballot=self.proposal_ballot,
            depth=self.depth,
            value=chosen_value,
            first_uncommitted=self._current_first_uncommitted(),
        )
        self.broadcast_func(accept_msg)

        # Seed local ACCEPTED from our own acceptor.
        acc = self.node_state.get_acceptor_state(self.depth)
        if self.proposal_ballot >= acc.ballot_num:
            acc.ballot_num = self.proposal_ballot
            acc.accept_num = self.proposal_ballot
            acc.accept_val = chosen_value

            # Log tentative for the leader's own acceptance
            if self.on_tentative is not None and chosen_value is not None:
                self.on_tentative(self.depth, chosen_value)

            local_accepted = PaxosMessage(
                msg_type=PaxosMessageType.ACCEPTED,
                from_id=self.node_state.node_id,
                ballot=self.proposal_ballot,
                depth=self.depth,
                value=chosen_value,
                first_uncommitted=self._current_first_uncommitted(),
            )
            self._on_accepted(local_accepted)

    def _on_accept(self, msg: PaxosMessage):
        acc = self.node_state.get_acceptor_state(self.depth)

        if msg.ballot < acc.ballot_num:
            print(
                f"[PAXOS depth={self.depth}] ACCEPT from {msg.from_id} with ballot={msg.ballot} "
                f"rejected (BallotNum={acc.ballot_num})",
                flush=True,
            )
            return

        # Accept this value.
        acc.ballot_num = msg.ballot
        acc.accept_num = msg.ballot
        acc.accept_val = msg.value

        accepted = PaxosMessage(
            msg_type=PaxosMessageType.ACCEPTED,
            from_id=self.node_state.node_id,
            ballot=msg.ballot,
            depth=self.depth,
            value=msg.value,
            first_uncommitted=self._current_first_uncommitted(),
        )

        print(
            f"[PAXOS depth={self.depth}] ACCEPT from {msg.from_id} accepted, "
            f"sending ACCEPTED for value={msg.value!r}",
            flush=True,
        )

        self.send_func(msg.from_id, accepted)

        # Log this as a tentative acceptance on this node
        if self.on_tentative is not None and msg.value is not None:
            self.on_tentative(self.depth, msg.value)

    def _on_accepted(self, msg: PaxosMessage):
        if self.is_decided:
            return

        if self.proposal_ballot is None:
            print(
                f"[PAXOS depth={self.depth}] ACCEPTED from {msg.from_id} ignored: "
                f"no active proposal",
                flush=True,
            )
            return

        if msg.ballot != self.proposal_ballot:
            print(
                f"[PAXOS depth={self.depth}] ACCEPTED from {msg.from_id} ignored: "
                f"ballot mismatch (msg={msg.ballot}, ours={self.proposal_ballot})",
                flush=True,
            )
            return

        if msg.from_id in self.accepted_received:
            # Duplicate ACCEPTED; ignore.
            return

        self.accepted_received[msg.from_id] = msg

        # Use first_uncommitted_index hint for eager repair.
        if msg.first_uncommitted is not None and self.repair_callback is not None:
            local_idx = self._current_first_uncommitted()
            peer_idx = int(msg.first_uncommitted)
            if local_idx is not None and peer_idx != local_idx:
                # Delegate repair logic to Node.
                self.repair_callback(msg.from_id, peer_idx, local_idx)

        print(
            f"[PAXOS depth={self.depth}] ACCEPTED from {msg.from_id} "
            f"(total={len(self.accepted_received)}/{self._quorum_size()})",
            flush=True,
        )

        # Check if we have a quorum of ACCEPTEDs.
        if len(self.accepted_received) < self._quorum_size():
            return

        # Majority has accepted our value; broadcast DECIDE.
        decide_msg = PaxosMessage(
            msg_type=PaxosMessageType.DECIDE,
            from_id=self.node_state.node_id,
            ballot=self.proposal_ballot,
            depth=self.depth,
            value=self.proposal_value,
            first_uncommitted=self._current_first_uncommitted(),
        )

        print(
            f"[PAXOS depth={self.depth}] ACCEPTED quorum reached, broadcasting DECIDE "
            f"value={self.proposal_value!r}",
            flush=True,
        )

        self.broadcast_func(decide_msg)
        # Apply decision locally as well.
        self._on_decide(decide_msg)

    def _on_decide(self, msg: PaxosMessage):
        if self.is_decided:
            return

        self.is_decided = True
        self.decided_value = msg.value
        print(f"[PAXOS depth={self.depth}] DECIDE value={msg.value!r}", flush=True)

        # Notify the owning Node that a value has been decided at this depth.
        self.on_decide(self.depth, msg.value)
