# Paxos-Based Fault-Tolerant Blockchain

Crash-fault-tolerant blockchain implemented with Paxos consensus and persistent recovery.

This project implements a private blockchain that processes money transfer transactions across multiple nodes. Each block is agreed upon using Paxos consensus to tolerate crash failures and ensure consistency. Nodes persist state to disk and can recover safely after failures while maintaining correct account balances across the system.

From the **project root directory**:

1. Open **5 separate terminals**.
2. In each terminal, run one of:

```bash
./run/run_p1.sh
./run/run_p2.sh
./run/run_p3.sh
./run/run_p4.sh
./run/run_p5.sh
```

## Features

- **Paxos-based consensus:** Uses Paxos to agree on each block height, ensuring a single consistent blockchain across nodes.
- **Crash fault tolerance:** Nodes may crash and restart without violating safety, assuming a majority remain alive.
- **Persistent recovery:** Blockchain state and Paxos metadata are written to disk and reloaded on restart.
- **Replicated ledger:** Each node maintains a local copy of the blockchain that converges through consensus.
- **Account-based transactions:** Supports money transfer operations with deterministic balance updates.
- **Multi-node execution:** Designed to run across multiple independent processes with configurable node identities.
- **Failure testing support:** Nodes can be stopped and restarted to validate recovery and consistency behavior.


