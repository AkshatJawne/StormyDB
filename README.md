# StormDB

A Redis-inspired, in-memory key-value database in Go. It features an RESP (Redis Serialization Protocol) parser for client communication, append-only file (AOF) persistence for crash recovery, and support for commands like like `SET`, `GET`, `HSET`, and `INCR`, Designed for low-latency performance, StormDB efficiently handles concurrent client connections.