(ns datomic-replication.core
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [datomic.api :as d]
            [enos.core :refer [dochan!]]))


;;; FIXME - need a marker per source+destination pair
(def marker-name "/datomic/replication/last-tx")

(defn- init-dest-database
  "This is the default implementation of the `:init` function that you
  can pass to `replicator`. It just creates the attribute in the
  destination database that connects each entity to its corresponding
  entity in the source database."
  [conn]
  @(d/transact conn
               [{:db/id                 (d/tempid :db.part/db)
                 :db/ident              :datomic-replication/source-eid
                 :db/valueType          :db.type/long
                 :db/unique             :db.unique/identity
                 :db/cardinality        :db.cardinality/one
                 :db.install/_attribute :db.part/db}]))

(defn- e->id-default
  "This is the default mechanism for getting a database-independent
  identifier for an entity. It uses a special attribute that gets
  installed in the destination database by the init function. If your
  source database has a unique identifier for entities, you can use
  that instead."
  [db eid]
  [:datomic-replication/source-eid eid])

(def default-opts
  {:init          init-dest-database
   :e->id         e->id-default
   :poll-interval 100})

(defn transactions
  "Returns an async channel of transactions.
   Options include:
    - from-t - the `t` to start at
    - poll interval - how long to pause when there are no new transactions
  "
  ([conn]
     (transactions conn nil))
  ([conn opts]
     (let [{:keys [from-t poll-interval]} (merge default-opts opts)
           ch        (async/chan)
           continue? (atom true)
           stopper   #(reset! continue? false)]
       (go-loop [from-t from-t]
         (when @continue?
           (let [txs (d/tx-range (d/log conn) from-t nil)]
             (if (seq txs)
               (do
                 (doseq [tx txs]
                   (>! ch tx))
                 (recur (inc (:t (last txs)))))
               (do
                 (<! (async/timeout poll-interval))
                 (recur from-t))))))
       [ch stopper])))


(defn partition-ident [db eid]
  (:db/ident (d/entity db (d/part eid))))

(defn skip-attr? [ident]
  false)

(defn tx->datoms
  "Transforms a transaction into a sequence of datoms suitable for
  transacting to the destination database."
  [{:keys [t data] :as tx} source-conn e->id]
  (let [source-db (d/as-of (d/db source-conn) t)]
    (for [[e a v t added?] data
          :let [[id-attr id-value] (e->id source-db e)
                part (partition-ident source-db e)
                attr-ident (:db/ident (d/entity source-db a))]
          :when (not (skip-attr? attr-ident))]
      (if added?
        (hash-map
         :db/id     (d/tempid part)
         id-attr    id-value
         attr-ident v)
        [:db/retract
         [id-attr id-value]
         attr-ident
         v]))))




(def ^:dynamic *e->id*
  "Function that returns the :ident of an attribute to use as the
  database-independent identifier for the given entity."
  (fn [db ent]
    (if-let [ident (:db/ident ent)]
      [:db/ident ident]
      [:datomic-replication/source-eid (:db/id ent)])))

(defn mapify-txs
  "Transforms the :data sequence from a transaction into a sequence of maps.
   [[e a v t added?] ...] => [{:eid e :added {a-ident v} :retracted {b-ident v}} ...]"
  [db data]
  (->> data
       (reduce (fn [m [e a v t added?]]
                 (let [attr-ident (:db/ident (d/entity db a))
                       action     (if added? :added :retracted)]
                   (assoc-in m [e action attr-ident] v)))
               nil)
       (map (fn [[eid m]] (assoc m :eid eid)))))

(defn assoc-partition
  [db m]
  (let [part (partition-ident db (:eid m))]
    (assoc m :part part)))

(defn assoc-identity
  "Given a map (as produced by mapify-txs), returns it with the ident
  of the entity assoc'ed in, if it has one."
  [db m]
  (if (= :db.part/tx (:part m))
    (assoc m :ident (d/tempid :db.part/tx))
    (let [[id-attr id-value] (*e->id* db (d/entity db (:eid m)))]
      (assoc m :ident [id-attr id-value]))))

(defn ->datoms
  [{:keys [eid ident added retracted]}]
  (concat [(when-not (empty? added)
             (assoc added :db/id ident))]
          (for [[attr v] retracted]
            [:db/retract
             ident
             attr
             v])))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;







;;; Transform the transaction by added some useful crap to it:
;;;  - mapping from source-eid -> tempid

(defn replicate-tx
  "Sends the transaction to the connection."
  [{:keys [t data] :as tx} source-conn dest-conn e->id]
  (prn "Got tx:" tx)
  (let [source-db (d/as-of (d/db source-conn) t)
        dest-db   (d/db dest-conn)
        eids      (doall (distinct (for [[e] data] e)))
        _  (prn "eids are " eids)

        ;; Create a mapping from the distinct eids in the transaction
        ;; to a tempid in the correct partition.
        tempids   (into {}
                        (for [eid eids]
                          (let [part (partition-ident source-db eid)]
                            [eid (d/tempid part)])))

        ;; A mapping from eids to idents, for those entities that have idents.
        idents    (into {}
                        (for [eid eids
                              :let [ent (d/entity source-db eid)
                                    ident (:db/ident ent)]
                              :when ident]
                          [eid ident]))
        
        datoms    (for [[e a v t added?] data
                        :let [[id-attr id-value] (e->id source-db e)
                              part (partition-ident source-db e)
                              attr-ident (:db/ident (d/entity source-db a))]
                        :when (not (skip-attr? attr-ident))]
                    (if added?
                      (if-let [ident (idents e)]
                        (hash-map
                         :db/id     ident
                         attr-ident v)
                        (hash-map
                         :db/id     (tempids e)
                         id-attr    id-value
                         attr-ident v))
                      [:db/retract
                       [id-attr id-value]
                       attr-ident
                       v]))

        ;; _         (->> tx
        ;;                :data
        ;;                (mapify-txs source-db)
        ;;                (map (partial assoc-partition source-db))
        ;;                (map (partial assoc-identity source-db))
        ;;                (mapcat ->datoms)
        ;;                (remove nil?))
        ]
    (prn "transacting:")
    (clojure.pprint/pprint datoms)
    (try
      @(d/transact dest-conn datoms)
      (catch Exception e
        (println "Error transacting!!!!")
        (.printStackTrace e)
        (throw e)))))


(defprotocol Replicator
  (start [this])
  (stop  [this]))

(defn replicator
  "Returns a replicator that copies transactions from source-conn to dest-conn."
  ([source-conn dest-conn]
     (replicator source-conn dest-conn nil))
  ([source-conn dest-conn opts]
     (let [{:keys [init e->id poll-interval] :as opts} (merge default-opts opts)
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
                   (init dest-conn))
                 (try
                   (replicate-tx tx source-conn dest-conn e->id)
                   (catch Exception e
                     (.printStackTrace e)
                     (throw e)))
                 (recur)))))
         (stop [this]
           (stopper)
           (async/put! control-chan :stop))))))




;;; Experimental - new middleware-based version

;; tx -> group by e? -> determine partitions -> get txInstant -> add tx metadata (source tx, ...)
;; -> recognize attr installs and add crap -> catch non-existing attrs and retry
