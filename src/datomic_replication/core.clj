(ns datomic-replication.core
  (:require [clojure.core.async :as async :refer [go go-loop <! >!]]
            [datomic.api :as datomic]
            [enos.core :refer [dochan!]]))


;;; FIXME - need a marker per source+destination pair
(def marker-name "/datomic/replication/last-tx")

(defn transactions
  "Returns an async channel of transactions, starting at `from-t`."
  ([conn]
     (transactions conn nil))
  ([conn from-t]
     (transactions conn from-t 1000))
  ([conn from-t poll-interval]
     (let [log (datomic/log conn)
           ch  (async/chan)]
       (go-loop [from-t from-t]
         (let [txs (datomic/tx-range log from-t nil)]
           (println "tx-range returned:" txs)
           (if (seq txs)
             (do
               (println "Got txs:" txs)
               (doseq [tx txs]
                 (println "Putting:" tx)
                 (>! ch tx))
               (recur (inc (:t (last txs)))))
             (do
               (<! (async/timeout poll-interval))
               (recur from-t)))))
       ch)))

(defn- init-dest-database
  "This is the default implementation of the `:init` function that you
  can pass to `replicator`. It just creates the attribute in the
  destination database that connects each entity to its corresponding
  entity in the source database."
  [conn]
  (datomic/transact conn
                    [{:db/ident              :datomic-replication/source-eid
                      :db/valueType          :db.type/long
                      :db/unique             :db.unique/identity
                      :db.install/_attribute :db.part/db}]))

(defn- e->id-default
  "This is the default mechanism for getting a database-independent
  identifier for an entity. It uses a special attribute that gets
  installed in the destination database by the init function. If your
  source database has a unique identifier for entities, you can use
  that instead."
  [db eid]
  [:datomic-replication/source-eid eid])


(defn replicate-tx
  "Sends the transaction to the connection."
  [{:keys [t data] :as tx} source-conn dest-conn e->id]
  (println "Got tx:" tx)
  (try
    (let [source-db (datomic/as-of (datomic/db source-conn) t)
          datoms    (for [[e a v t added?] data]
                      [(if added? :db/add :db/retract)
                       (e->id source-db e)
                       (:db/ident (datomic/entity source-db a))
                       v])]
      (datomic/transact dest-conn datoms))
    (catch Exception e
      (.printStackTrace e)
      (throw e))))


(defprotocol Replicator
  (start [this])
  (stop  [this]))

(def default-opts
  {:init          init-dest-database
   :e->id         e->id-default
   :poll-interval 1000})

(defn replicator
  "Returns a replicator that copies transactions from source-conn to dest-conn."
  ([source-conn dest-conn]
     (replicator source-conn dest-conn nil))
  ([source-conn dest-conn opts]
     (let [{:keys [init e->id poll-interval]} (merge default-opts opts)
           control-chan (async/chan)
           transactions (transactions source-conn poll-interval)
           initialized? (atom false)]
       (reify Replicator
         (start [this]
           (go-loop []
             (let [[tx ch] (async/alts! [transactions control-chan])]
               (when (identical? ch transactions)
                 (when-not @initialized?
                   (reset! initialized? true)
                   (init dest-conn))
                 (replicate-tx tx source-conn dest-conn e->id)
                 (recur)))))
         (stop [this]
           (async/put! control-chan :stop))))))
