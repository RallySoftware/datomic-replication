# datomic-replication

Replicate a datomic database to another datomic database.

## Why?

We developed this at Rally Software to support fast failover from a
hot to a warm datacenter.


## How Does it Work?

Let's suppose that we have a Datomic database called "source" which is
taking production traffic. We want another database, called "dest",
that needs to be a near-realtime copy of "source".

On the surface, this appears to be reasonably simple to implement with
the tools that Datomic provides - we just look at each transaction
going into the source database, and replay it to the
destination.

There are (at least) two ways that we can get access to
the transactions in the source:

  - datomic/tx-report-queue
  - The log API

We can immediately dismiss tx-report-queue for this usage because it
only gives access to "live" transactions - there is no access to
history, or capability to "catch up" if our replicator goes down for a
while.

So we will use the log api. This allows us to get at a sequence of
transactions, in chronological order, from any point in the history of
the database.

Each transaction consists of a `t` value and a sequence of
datoms. Each datom is a five-element list which should look very
familiar if you use Datoimc.

```clojure
  [e       ;; Entity id
   a       ;; Attribute id
   v       ;; Value
   t       ;; Transaction
   added?] ;; Boolean - true for :db/add, false for :db/retract
```

At first, it seems like we can just do a simple transformation on
these datoms and then transact them into the destination database:

```clojure
  (doseq [tx (d/tx-range (d/log source-conn) start-t end-t)]
    (d/transact dest-conn
      (for [[e a v t added?] (:data tx)]
        [(if added? :db/add :db/retract)
         e
         a
         v])))
```

Unfortunately it is not this easy - the `e` and `a` values are ids
that are internal to the source database. They are not valid in the
destination database, so this transaction will fail.

Solving this for the `a` is easy, so let's do that first. Attributes
in Datomic have an `ident` property that uniquely identifies them, so
we can use this as our cross-database identifier.

```clojure
  (doseq [tx (d/tx-range (d/log source-conn) start-t end-t)]
    (let [source-db (d/as-of (d/db source-conn) (:t tx))]
      (d/transact dest-conn
        (for [[e a v t added?] (:data tx)]
          [(if added? :db/add :db/retract)
           e
           (:db/ident (d/entity source-db a))
           v]))))
```

We can do this because Datomic allows us to use the :db/ident property
of an attribute in place of its internal id. In fact, this is probably
how you are used to referring to attributes.

Next, we need to solve the entity-id problem. This is trickier,
because entities in Datomic do not, by default, have any globally
unique properties. This means that entity 123 in the source database
may be, conceptually, the "same" as entity 456 in the destination
database, but we have no built-in way to make that correlation.

There are two ways that we are able to solve this. One is to
manufacture an artificial correspondence between the source and
destination entities. This is the default behavior of this library.

The second, and preferable, solution is to use a unique attribute from
your domain to link the entities.

Let's start with an example of the second solution. Suppose that your
database has a unique attribute - that is, one created with the
`db/unique` property set to `db.unique/identity` - and that every
entity has a value for this attribute. Let's call this unique
attribute `:user/guid`. Then, taking advantage of
[Lookup Refs](http://docs.datomic.com/identity.html#lookup-refs), we
can do the following:

```clojure
  (doseq [tx (d/tx-range (d/log source-conn) start-t end-t)]
    (let [source-db (d/as-of (d/db source-conn) (:t tx))]
      (d/transact dest-conn
        (for [[e a v t added?] (:data tx)
              :let [guid (:user/guid (d/entity source-db e))]]
          [(if added? :db/add :db/retract)
           [:user/guid guid]
           (:db/ident (d/entity source-db a))
           v]))))
```

Notice how we have used a database-independent identity: the
lookup-ref `[:user/guid guid]`. This can be used in place of an actual
entity-id in many places in Datomic's API.

This works... sort of. In limited situations. The problem now is that
lookup-refs are only valid for... well, for looking stuff up. They
cannot be used to create a new entity. So our latest version will work
to transact new properties of entities that already exist in the
destination database, but will not work for creating new
entities.

Let's formulate our transaction a little differently.

Instead of using the lookup-ref as an id, we will use the "upsert"
capability imparted by the `:db.unique/identity` property to transact
a change that will succeed whether or not the destination entity
exists already.

```clojure
  (doseq [tx (d/tx-range (d/log source-conn) start-t end-t)]
    (let [source-db (d/as-of (d/db source-conn) (:t tx))]
      (d/transact dest-conn
        (for [[e a v t added?] (:data tx)
              :let [guid (:user/guid (d/entity source-db e))]]
          {:db/id (d/tempid :db.part/???)
           :user/guid guid
           (:db/ident (d/entity source-db a)) v}))))
```

This works! Well... sort-of. It does allow us to handle both new and
existing entities. The tempid is effectively ignored because the
:user/guid property uniquely identifies the entity. However, you'll
notice that we have now glossed over a couple of important details:
partitions and retractions.

Partitions are pretty easy to deal with. Given an entity-id from the source
database, we can find out the `ident` of the partition that it lives in.

```clojure
  (defn parition-ident [db eid]
    (:db/ident (d/entity db (datomic/part eid))))

  (...
    (for [[e a v t added?] (:data tx)
          :let [guid (:user/guid (d/entity source-db e))]]
      {:db/id (d/tempid (partition-ident source-db e))
       :user/guid guid
       (:db/ident (d/entity source-db a)) v})
       ...)
```

Now what about retractions? Passing maps to transact only works for
:db/add operations, not for :db/retract. However, note that we only
had to switch to maps to deal with entites that do not yet exist in
the destination. Since we will never see a retraction for a
non-existent entity, and assuming that the destination database is a
faithful copy of the source up to the point in history that we are
currently transacting, we can safely continue using vectors with
lookup-refs for retractions.

```clojure
  (doseq [tx (d/tx-range (d/log source-conn) start-t end-t)]
    (let [source-db (d/as-of (d/db source-conn) (:t tx))]
      (d/transact dest-conn
        (for [[e a v t added?] (:data tx)
              :let [guid (:user/guid (d/entity source-db e))]]
          (if added?
            {:db/id (d/tempid :db.part/???)
             :user/guid guid
             (:db/ident (d/entity source-db a)) v}
            [:db/retract
             [:user/guid guid]
             (:db/ident (d/entity source-db a))
             v])))))
```


TODO: Finish / fix this document. I wrote the above before writing the
code, and it ended up working a little bit differently.

## License

Copyright © 2014 Rally Software

Distributed under the MIT License
