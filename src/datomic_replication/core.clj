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

(def ^:dynamic e->id-default
  "Function that returns the :ident of an attribute to use as the
  database-independent identifier for the given entity."
  (fn [db ent]
    (if-let [ident (:db/ident ent)]
      [:db/ident ident]
      [:datomic-replication/source-eid (:db/id ent)])))

(defn default-opts []
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
     (let [{:keys [from-t poll-interval]} (merge (default-opts) opts)
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

(defn skip-attr? [attr]
  false)


(defn replicate-tx
  "Sends the transaction to the connection."
  [{:keys [t data] :as tx} source-conn dest-conn e->id]
  (prn "Got tx:")
  (clojure.pprint/pprint tx)
  (let [source-db (d/as-of (d/db source-conn) t)
        dest-db   (d/db dest-conn)
        eids      (doall (distinct (for [[e] data] e)))
        _         (prn "eids are " eids)

        ;; Create a mapping from the distinct eids in the transaction
        ;; to a tempid in the correct partition.
        e->temp   (into {}
                        (for [eid eids]
                          (let [part (partition-ident source-db eid)]
                            [eid (d/tempid part)])))

        datoms    (for [[e a v t added?] data
                        :let [ent              (d/entity source-db e)
                              [id-attr id-val] (e->id source-db ent)
                              attr             (d/entity source-db a)
                              attr-ident       (:db/ident attr)
                              is-ref?          (= :db.type/ref (:db/valueType attr))
                              v                (if is-ref?
                                                 (or (e->temp v) v)
                                                 v)]
                        :when (not (skip-attr? attr))]
                    (if added?
                      (if (= [:db/ident :db.part/db] [id-attr id-val]) ; FIXME - too specific
                        (hash-map
                         :db/id     id-val
                         attr-ident v)
                        (hash-map
                         :db/id     (e->temp e)
                         id-attr    id-val
                         attr-ident v))
                      [:db/retract
                       [id-attr id-val]
                       attr-ident
                       v]))]
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
     (let [{:keys [init e->id poll-interval] :as opts} (merge (default-opts) opts)
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
                 (try
                   (replicate-tx tx source-conn dest-conn e->id)
                   (catch Exception e
                     (.printStackTrace e)
                     (throw e)))
                 (recur)))))
         (stop [this]
           (stopper)
           (async/put! control-chan :stop))))))
