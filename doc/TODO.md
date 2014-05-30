# TODO

  - Logging
  - Metadata persistence. Track, in the destination datomic, metadata about which transactions have been replicated. The simplest possiblity is to just store the txInstant of the last-replicated tx. Restart from there next time around.
  - Fix ref attributes to use refer to the referee in the destination db. Use lookup-ref.
