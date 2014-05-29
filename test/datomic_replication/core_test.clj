(ns datomic-replication.core-test
  (:require [clojure.test :refer :all]
            [datomic.api :as datomic]
            [datomic-replication.core :as rep]))


(deftest test-replication
  (let [uri1 "datomic:free://localhost:4334/source"
        uri2 "datomic:free://localhost:4334/dest"]
    (try
      (datomic/create-database uri1)
      (datomic/create-database uri2)
      (let [db1        (datomic/connect uri1)
            db2        (datomic/connect uri2)
            replicator (rep/replicator db1 db2)]
      
        ;; Start the replication
        (rep/start replicator)

        (try
          ;; Do some stuff to the source database:
          (let [result @(datomic/transact db1 [{:db/id                 (datomic/tempid :db.part/db)
                                                :db/ident              :user/name
                                                :db/valueType          :db.type/string
                                                :db/cardinality        :db.cardinality/one
                                                :db.install/_attribute :db.part/db}])]
            (is result))

          ;; Wait a bit for it to replicate
          (Thread/sleep 3000)

          ;; And make sure that the attribute got replicated
          (is (= 1 (count (datomic/datoms :avet :db/ident :user/name))))

        
          (finally
            (rep/stop replicator))))

      (finally
        (datomic/delete-database uri1)
        (datomic/delete-database uri2)))))

