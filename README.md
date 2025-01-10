# hanfried-db

Rust DBMS with focus on minimal space requirements and out of box (distributed) working

## Goals

* Learn Rust
* Light SQL SB with focus on:
  * Minimal ram and disk usage: ram and disks are both slow, while computation is fast.
    Real life data has low entropy and can be stored very efficiently to reduce slow ram and very slow disk usage.
    Also, in real scenarios, it's not only about performance, but also minimizing requirements for deployments,
    e.g. in typical tight kubernetes setups found in enterprises
  * Out of box concurrency (parallel transactions) and distributed computing
  * Minimal (opinionated) setup need
  * No focus on supporting edge features or stored procedures or similar: in usual devops environment it's not needed
    anyway and for (slow, space and time-consuming) data warehousing, there are much better alternatives  
* Should compare to Redis, Sqlite and Elasticsearch

## Disclaimer

No experience writing rust applications or dbms, main goal still is learning some techniques, the other goals
are just giving a roadmap.
