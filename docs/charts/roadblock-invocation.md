# Typical roadblock invocation

This sequence shows one normal barrier invocation with one leader and one of
the `N` followers. The other followers perform the same follower-side steps
in parallel. Redis Streams carry the messages; the leader consumes its leader
and personal streams, while each follower consumes the global, followers, and
personal streams.

```mermaid
flowchart LR
    subgraph Leader[Leader invocation]
        L0[Start roadblocker.py<br/>--role leader<br/>--followers F1 ... FN]
        L1[Connect to Redis]
        L2{Create roadblock key?}
        L3[Create global, leader,<br/>and followers streams<br/>publish timeout-ts]
        L4[Create leader personal stream]
        L5[Send leader-online<br/>to followers stream]
        L6[Wait for follower-online<br/>from F1 ... FN]
        L7[Send all-online]
        L8[Track follower-ready<br/>from F1 ... FN]
        L9[Send all-ready]
        L10{Decision from followers}
        L11[Send all-go]
        L12[Send all-abort]
        L13[Send all-wait<br/>then leader-heartbeat]
        L14[Track follower-heartbeat<br/>and waiting-complete]
        L15[Send all-go]
        L16[Track follower-gone<br/>from F1 ... FN]
        L17[Send all-gone<br/>delete keys and streams]
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
        F0[Start roadblocker.py<br/>--role follower<br/>--follower-id F1]
        F1[Connect to Redis]
        F2[See existing key or wait<br/>for initialized flag]
        F3[Create F1 personal stream]
        F4[Send follower-online<br/>to leader stream]
        F5[Receive all-online]
        F6[Send follower-ready<br/>or follower-ready-abort<br/>or follower-ready-waiting]
        F7{Leader decision}
        F8[Receive all-go]
        F9[Receive all-abort<br/>kill wait-for if active]
        F10[Receive all-wait]
        F11[Reply follower-heartbeat<br/>until wait-for completes]
        F12[Send follower-waiting-complete<br/>or ...-failed]
        F13[Send follower-gone<br/>stop watching streams]
        F14([Return success or abort])
        F0 --> F1 --> F2 --> F3 --> F4 --> F5 --> F6 --> F7
        F7 -- all-go --> F8 --> F13
        F7 -- all-abort --> F9 --> F13
        F7 -- all-wait --> F10 --> F11 --> F12
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
    classDef store fill:#fff4cc,stroke:#a87900,color:#111;
    classDef terminal fill:#e2f3e5,stroke:#38734a,color:#111;
    class L0,L1,L2,L3,L4,L5,L6,L7,L8,L9,L10,L11,L12,L13,L14,L15,L16,L17 leader;
    class F0,F1,F2,F3,F4,F5,F6,F7,F8,F9,F10,F11,F12,F13 follower;
    class R0,R1,R2,R3,R4 store;
    class L18,F14 terminal;
```
