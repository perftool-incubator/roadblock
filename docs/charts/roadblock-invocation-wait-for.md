# Wait-for invocation path

This focused view shows the optional `--wait-for` path. The leader replaces
the fixed barrier timeout with heartbeat monitoring until the follower's
long-running operation completes.

```mermaid
%%{init: {"sequence": {"useMaxWidth": true}}}%%
sequenceDiagram
    autonumber
    participant L as Leader
    participant R as Redis / Valkey
    participant F as F1 of N followers

    Note over L,F: Initialize and join
    L->>R: Create keys and streams<br/>publish <strong>timeout-ts</strong>
    F->>R: Create personal stream<br/>start --wait-for command
    L->>R: <strong>leader-online</strong>
    F->>R: <strong>follower-online</strong>
    R-->>L: <strong>follower-online</strong> from F1 ... FN

    Note over L,F: Enter wait-for mode
    L->>R: Broadcast <strong>all-online</strong>
    R-->>F: <strong>all-online</strong>
    F->>R: <strong>follower-ready-waiting</strong>
    R-->>L: <strong>follower-ready-waiting</strong> from a follower
    L->>R: Broadcast <strong>all-ready</strong>
    L->>R: Broadcast <strong>all-wait</strong>
    R-->>F: <strong>all-wait</strong>

    loop Until all wait-for commands complete
        L->>R: Broadcast <strong>leader-heartbeat</strong>
        R-->>F: <strong>leader-heartbeat</strong>
        F->>R: <strong>follower-heartbeat</strong>
        R-->>L: <strong>follower-heartbeat</strong>
    end

    F->>R: <strong>follower-waiting-complete</strong>
    R-->>L: <strong>follower-waiting-complete</strong> from F1 ... FN
    L->>R: Broadcast <strong>all-go</strong>
    R-->>F: <strong>all-go</strong>

    Note over L,F: Exit and cleanup
    F->>R: <strong>follower-gone</strong>
    R-->>L: <strong>follower-gone</strong> from F1 ... FN
    L->>R: Broadcast <strong>all-gone</strong><br/>delete keys and streams
```
