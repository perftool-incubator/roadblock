# roadblock
A synchronization services tool

## Introduction
Roadblock provides synchronization services for multiple lines of execution, most likely in a distributed system (systems, virtual machines, containers, etc.).  A centralized Redis server is used to provide communication services between a single 'leader' and one or more 'followers'.  The 'leader' is responsible for ensuring that all members of the roadblock have reached a common state (ie. 'ready') before releasing them with a 'go' command.  Each member confirms it's receipt of the 'go' command by responding with a 'gone' command before proceeding.

## Documentation

### Protocol

![Roadblock protocol flow chart](docs/charts/roadblock-protocol.svg)

### Bus Mirroring

![Roadblock bus mirroring flow chart](docs/charts/roadblock-bus-mirroring.svg)
