# Roadblock - Distributed Synchronization

## Purpose
Roadblock provides barrier synchronization and message passing for distributed benchmark execution. It uses Redis as the communication backend, enabling multiple processes across hosts to synchronize at defined points during a test run.

## Language
Python — core library + CLI

## Key Files
| File | Purpose |
|------|---------|
| `roadblock.py` | Core library (~2000 lines) — barrier sync, message handling, heartbeat |
| `roadblocker.py` | CLI entry point for running roadblock operations |
| `roadblocker_config.py` | Configuration handling |
| `utilities/redis-monitor.py` | Redis monitoring utility |
| `utilities/run-pylint.sh` | Linting script |
| `workshop.json` | Image build requirements (roadblock runs on both controller and engines — controller as leader, engines/endpoints as followers) |

## Tests
Run via `test/run-test.sh` — launches a Podman pod with Redis and tests multiple scenarios:
- Basic synchronization (50 followers)
- Timeout handling
- Abort scenarios
- Wait-for patterns
- Heartbeat timeout
- Leader SIGINT handling

Build the test container: `test/build-container.sh`

## Return Codes
| Code | Constant | Meaning |
|------|----------|---------|
| 0 | `RC_SUCCESS` | Successful synchronization |
| 1 | `RC_ERROR` | General error |
| 2 | `RC_INVALID_INPUT` | Invalid arguments |
| 3 | `RC_TIMEOUT` | Barrier timeout |
| 4 | `RC_ABORT` | Abort (single SIGINT) |
| 5 | `RC_HEARTBEAT_TIMEOUT` | Heartbeat timeout (wait-for) |
| 6 | `RC_ABORT_WAITING` | Abort while waiting (wait-for) |

## Conventions
- Primary branch is `master`
- **Leader-follower model**: One leader coordinates with N followers at each barrier
- Messages are JSON-formatted, compressed with lzma for large payloads
- Redis pub/sub for real-time coordination, keys for state persistence
- Configurable timeout, heartbeat interval, and abort behavior
