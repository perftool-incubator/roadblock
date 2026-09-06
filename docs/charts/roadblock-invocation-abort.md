# Abort invocation path

This focused view shows a follower reporting an error before the barrier is
released. The leader propagates the abort, and all followers still complete
the gone handshake.

```mermaid
%%{init: {"sequence": {"useMaxWidth": true}}}%%
sequenceDiagram
    autonumber
    participant L as Leader
    participant R as Redis / Valkey
    participant F as F1 of N followers

    Note over L,F: Initialize and join
    L->>R: Create keys and streams<br/>publish <strong>timeout-ts</strong>
    F->>R: Create personal stream
    L->>R: <strong>leader-online</strong>
    F->>R: <strong>follower-online</strong>
    R-->>L: <strong>follower-online</strong> from F1 ... FN

    Note over L,F: Reach the decision point
    L->>R: Broadcast <strong>all-online</strong>
    R-->>F: <strong>all-online</strong>
    F->>R: <strong>follower-ready-abort</strong>
    R-->>L: <strong>follower-ready-abort</strong> from a follower
    L->>R: Broadcast <strong>all-ready</strong>
    L->>R: Broadcast <strong>all-abort</strong>
    R-->>F: <strong>all-abort</strong>

    Note over L,F: Abort and exit
    F->>R: <strong>follower-gone</strong>
    R-->>L: <strong>follower-gone</strong> from F1 ... FN
    L->>R: Broadcast <strong>all-gone</strong><br/>delete keys and streams
```
