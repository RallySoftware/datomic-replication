# TODO

  - Logging DONE (somewhat)
  - Metadata persistence. Track, in the destination datomic, metadata
    about which transactions have been replicated. The simplest
    possiblity is to just store the txInstant or t of the
    last-replicated tx. Restart from there next time around. (DONE)
  - Fix ref attributes to use refer to the referee in the destination
    db. Use lookup-ref. (DONE)
  - Handle transaction timeout. Pause then retry? (DONE)
  - Filtering - configurable omission of data - maybe - this could
    lead to unresovled refs if you are not careful
  
Crazier ideas:
  - Splitting and merging of databases.
  - Pruning history
  - Re-partitioning
  
