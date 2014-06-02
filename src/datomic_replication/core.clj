(ns datomic-replication.core
  "Datomic Replication"
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [datomic.api :as d]
            [enos.core :refer [dochan!]])
  (:import [java.util Date]))


(defn- init-dest-database
  "This is the default implementation of the `:init` function that you
  can pass to `replicator`. 

  It creates the attribute in the
  destination database that connects each entity to its corresponding
  entity in the source database. It also gets a transaction from the
  source database as a parameter, so that it can use the
  txInstant. This is necessary because we need to set the timestamp of
  this initialization-transaction to be earlier than the first
  transaction that we will replicate from teh source."
  [conn tx]
  @(d/transact conn
               [{:db/id                 (d/tempid :db.part/tx)
                 ;; tx is the first transaction from the source database. We
                 :db/txInstant          (Date. (- (.getTime (:db/txInstant tx)) 1000))}
                {:db/id                 (d/tempid :db.part/db)
                 :db/ident              :datomic-replication/source-eid
                 :db/valueType          :db.type/long
                 :db/unique             :db.unique/identity
                 :db/cardinality        :db.cardinality/one
                 :db.install/_attribute :db.part/db}]))

(def ^:dynamic e->lookup-ref-default
  "Function that returns the :ident of an attribute to use as the
  database-independent identifier for the given entity."
  (fn [db ent]
    (if-let [ident (:db/ident ent)]
      [:db/ident ident]
      [:datomic-replication/source-eid (:db/id ent)])))


(defn transactions
  "Returns an async channel of transactions.
   Options include:
    - from-t - the `t` to start at
    - poll-interval - how long to pause when there are no new transactions
  "
  ([conn]
     (transactions conn {:from-t        nil
                         :poll-interval 100}))
  ([conn opts]
     (let [{:keys [from-t poll-interval]} opts
           ch        (async/chan)
           continue? (atom true)
           stopper   #(reset! continue? false)]
       (go-loop [from-t from-t]
         (when @continue?
           (let [txs (d/tx-range (d/log conn) from-t nil)]
             (if (seq txs)
               (do
                 (doseq [tx txs]
                   (when @continue?
                     (>! ch tx)))
                 (recur (inc (:t (last txs)))))
               (do
                 (<! (async/timeout poll-interval))
                 (recur from-t))))))
       [ch stopper])))


(defn partition-ident [db eid]
  (:db/ident (d/entity db (d/part eid))))

(defn skip-attr? [attr]
  false)

(defn- log [msg data]
  ;;(println msg)
  ;;(clojure.pprint/pprint data)
  )

(defn replicate-tx
  "Sends the transaction to the connection."
  [{:keys [t data] :as tx} source-conn dest-conn e->lookup-ref]
  (log "Got tx:" tx)
  (let [source-db    (d/as-of (d/db source-conn) t)
        dest-db      (d/db dest-conn)
        eids         (distinct (for [[e] data] e))

        ;; Mapping from each distinct eid to a database-independent
        ;; identifier for the entity - a lookup-ref in the form:
        ;; [attr-ident attr-value]. This will be one of:
        ;;  - [:db/ident <val>], for entities that have an ident
        ;;  - [:datomic-replication/source-eid <eid>] (default)
        ;;  - a domain-specific unique attribute+value pair
        ->lookup-ref (memoize
                      (fn [eid]
                        (e->lookup-ref source-db (d/entity source-db eid))))

        ;; Function to translate an eid from the source database into
        ;; one that is valid in the destination database.  This will
        ;; be either an actual eid, if the entity exists already, or a
        ;; tempid if the entity is new.
        ->dest-eid   (memoize
                      (fn [eid]
                        (let [lookup-ref (->lookup-ref eid)
                              dest-ent   (d/entity dest-db lookup-ref)]
                          (if dest-ent
                            (:db/id dest-ent)
                            (let [part (partition-ident source-db eid)]
                              (d/tempid part))))))

        datoms       (for [[e a v t added?] data
                           :let [[id-attr id-val] (->lookup-ref e)
                                 attr             (d/entity source-db a)
                                 attr-ident       (:db/ident attr)
                                 is-ref?          (= :db.type/ref (:db/valueType attr))
                                 v                (if is-ref?
                                                    (->dest-eid v)
                                                    v)]
                           :when (not (skip-attr? attr))]
                       (if added?
                         (hash-map
                          :db/id     (->dest-eid e)
                          id-attr    id-val
                          attr-ident v)
                         [:db/retract
                          [id-attr id-val]
                          attr-ident
                          v]))]
    (log "transacting:" datoms)
    (try
      @(d/transact dest-conn datoms)
      (catch Exception e
        (println "Error transacting!!!!")
        (.printStackTrace e)
        (throw e)))))


(defprotocol Replicator
  (start [this])
  (stop  [this]))

(defn default-opts []
  {:init          init-dest-database
   :e->lookup-ref e->lookup-ref-default
   :poll-interval 100
   :start-t       nil})

(defn replicator
  "Returns a replicator that copies transactions from source-conn to dest-conn.
  Call `start`, passing the returned object, to begin replication.
  Call `stop` to stop replication.
  `opts` is a map that can have the the following keys (all optional):
   - :init - function to initialize the destination database. Will be passed 2
            arguments: [dest-conn tx], where `tx` is the first transaction to be
            replicated. The default is `init-dest-database`, which creates the
            :datomic-replication/source-eid attribute with a :txInstant one second
            earlier than `tx`.
   - :e->lookup-ref - Function to return a lookup-ref, given a db and an eid. The
                      default returns [:db/ident <ident>] if the entity has an ident,
                      or [:datomic-replication/source-eid <eid>] otherwise.
   - :poll-interval - Number of milliseconds to wait before calling `tx-range` again
                     after calling it and getting no transactions. This determines
                     how frequently to poll when we are \"caught up\".
   - :from-t - The `t` to start from. Default is nil, which means to start at the 
               beginning of the source database's history, or at the last-t stored
               in the destination database.
  "
  ([source-conn dest-conn]
     (replicator source-conn dest-conn nil))
  ([source-conn dest-conn opts]
     (let [{:keys [init
                   e->lookup-ref
                   poll-interval
                   from-t] :as opts} (merge (default-opts) opts)
           control-chan  (async/chan)
           [txs stopper] (transactions source-conn opts)
           initialized?  (atom false)]
       (reify Replicator
         (start [this]
           (go-loop []
             (let [[tx ch] (async/alts! [txs control-chan])]
               (when (identical? ch txs)
                 (when-not @initialized?
                   (reset! initialized? true)
                   (init dest-conn (-> tx :data first (nth 3) (->> (d/entity (d/db source-conn))))))
                   (replicate-tx tx source-conn dest-conn e->lookup-ref)
                 (try
                   (catch Exception e
                     (.printStackTrace e)
                     (throw e)))
                 (recur)))))
         (stop [this]
           (stopper)
           (async/put! control-chan :stop))))))
