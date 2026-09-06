# Typical roadblock invocation

This sequence shows one normal barrier invocation with one leader and one of
the `N` followers. The other followers perform the same follower-side steps
in parallel. Redis Streams carry the messages; the leader consumes its leader
and personal streams, while each follower consumes the global, followers, and
personal streams.

```mermaid
%%{init: {"flowchart": {"useMaxWidth": true, "htmlLabels": true}}}%%
flowchart LR
    subgraph Leader[Leader invocation]
        L0[Start roadblocker.py<br/><strong>--role leader</strong><br/><strong>--followers F1 ... FN</strong>]
        L1[Connect<br/>to Redis]
        L2{Create<br/>roadblock key?}
        L3[Create global, leader,<br/>and followers streams<br/>publish <strong>timeout-ts</strong>]
        L4[Create leader<br/>personal stream]
        L5[Send <strong>leader-online</strong><br/>to followers stream]
        L6[Wait for <strong>follower-online</strong><br/>from F1 ... FN]
        L7[Send <strong>all-online</strong>]
        L8[Track <strong>follower-ready</strong><br/>from F1 ... FN]
        L9[Send <strong>all-ready</strong>]
        L10{Decision<br/>from followers}
        L11[Send <strong>all-go</strong>]
        L12[Send <strong>all-abort</strong>]
        L13[Send <strong>all-wait</strong><br/>then <strong>leader-heartbeat</strong>]
        L14[Track <strong>follower-heartbeat</strong><br/>and <strong>follower-waiting-complete</strong>]
        L15[Send <strong>all-go</strong>]
        L16[Track <strong>follower-gone</strong><br/>from F1 ... FN]
        L17[Send <strong>all-gone</strong><br/>delete keys<br/>and streams]
        L18([Return success])
        L0 --> L1 --> L2
        L2 -- no --> L3 --> L4
        L2 -- yes --> L4
        L4 --> L5 --> L6 --> L7 --> L8 --> L9 --> L10
        L10 -- normal --> L11
        L10 -- abort --> L12
        L10 -- wait-for --> L13 --> L14 --> L15
        L11 --> L16
        L12 --> L16
        L15 --> L16 --> L17 --> L18
    end

    subgraph Redis[Redis / Valkey Streams]
        R0[(Roadblock keys)]
        R1[(global stream)]
        R2[(followers stream)]
        R3[(leader stream)]
        R4[(personal streams<br/>leader, F1 ... FN)]
    end

    subgraph Follower[One follower: F1]
        F0[Start roadblocker.py<br/><strong>--role follower</strong><br/><strong>--follower-id F1</strong>]
        F1[Connect<br/>to Redis]
        F2[See existing key<br/>or wait for<br/>initialized flag]
        F3[Create F1<br/>personal stream]
        F4[Send <strong>follower-online</strong><br/>to leader stream]
        F5[Receive <strong>all-online</strong>]
        F6[Send <strong>follower-ready</strong><br/>or <strong>follower-ready-abort</strong><br/>or <strong>follower-ready-waiting</strong>]
        F7{Leader<br/>decision}
        F8[Receive <strong>all-go</strong>]
        F9[Receive <strong>all-abort</strong><br/>kill wait-for<br/>if active]
        F10[Receive <strong>all-wait</strong>]
        F11[Reply <strong>follower-heartbeat</strong><br/>until wait-for completes]
        F12[Send <strong>follower-waiting-complete</strong><br/>or <strong>...-failed</strong>]
        F13[Send <strong>follower-gone</strong><br/>stop watching<br/>streams]
        F14([Return success or abort])
        F0 --> F1 --> F2 --> F3 --> F4 --> F5 --> F6 --> F7
        F7 -- "<strong>all-go</strong>" --> F8 --> F13
        F7 -- "<strong>all-abort</strong>" --> F9 --> F13
        F7 -- "<strong>all-wait</strong>" --> F10 --> F11 --> F12
        F12 --> F13
        F13 --> F14
    end

    L1 -. reads/writes .-> R0
    L3 -. creates .-> R1
    L3 -. creates .-> R2
    L3 -. creates .-> R3
    L4 -. creates .-> R4
    L5 -. writes .-> R2
    L6 -. reads .-> R3
    L7 -. writes .-> R2
    L8 -. reads .-> R3
    L9 -. writes .-> R2
    L11 -. writes .-> R2
    L12 -. writes .-> R2
    L13 -. writes .-> R2
    L14 -. reads .-> R3
    L16 -. reads .-> R3
    F1 -. reads/writes .-> R0
    F3 -. creates .-> R4
    F4 -. writes .-> R3
    F5 -. reads .-> R2
    F6 -. writes .-> R3
    F8 -. reads .-> R2
    F9 -. reads .-> R2
    F10 -. reads .-> R2
    F11 -. reads .-> R2
    F11 -. writes .-> R3
    F12 -. writes .-> R3
    F13 -. writes .-> R3

    classDef leader fill:#e8f1fb,stroke:#356a9a,color:#111;
    classDef follower fill:#f4eafb,stroke:#7a4b9a,color:#111;
    classDef state fill:#dbeafe,stroke:#2563eb,stroke-width:3px,color:#111;
    classDef store fill:#fff4cc,stroke:#a87900,color:#111;
    classDef terminal fill:#e2f3e5,stroke:#38734a,color:#111;
    class L0,L1,L2,L3,L4,L5,L6,L7,L8,L9,L10,L11,L12,L13,L14,L15,L16,L17 leader;
    class F0,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12,F13 follower;
    class L2,L10,F7 state;
    class R0,R1,R2,R3,R4 store;
    class L18,F14 terminal;
```
