(ns datomic-replication.core-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [datomic-replication.core :as rep]))



(defmacro with-databases [& body]
  `(let [~'uri1 "datomic:free://localhost:4334/source"
         ~'uri2 "datomic:free://localhost:4334/dest"]
     (testing "Schema already set up in destination"
       (try
         (d/create-database ~'uri1)
         (d/create-database ~'uri2)
         (let [~'c1         (d/connect ~'uri1)
               ~'c2         (d/connect ~'uri2)
               ~'replicator (rep/replicator ~'c1 ~'c2)]
      
           ~@body)

         (finally
           (d/delete-database ~'uri1)
           (d/delete-database ~'uri2))))))


(defn define-attr [conn ident type & [opts]]
  (let [type (keyword "db.type" (name type))]
    @(d/transact conn [(merge {:db/id                 (d/tempid :db.part/db)
                               :db/ident              ident
                               :db/valueType          type
                               :db/cardinality        :db.cardinality/one
                               :db.install/_attribute :db.part/db}
                              opts)])))

(deftest test-replication-1
  (with-databases
    ;; Start the replication
    (rep/start replicator)

    (try
      ;; Create an attribute in the source database
      (define-attr c1 :user/name :string)

      ;; Wait a bit for it to replicatenre
      (Thread/sleep 500)

      ;; And make sure that the attribute got replicated
      (is (= 1 (count (seq (d/datoms (d/db c2) :avet :db/ident :user/name)))))

      
      (finally
        (rep/stop replicator)))))

(deftest test-replication-2
  (with-databases
    ;; Start the replication
    (rep/start replicator)

    (try
      ;; Create an attribute in the source database
      (define-attr c1 :user/name :string)
      (define-attr c1 :user/age :long)

      @(d/transact c1 [{:db/id (d/tempid :db.part/user)
                              :user/name "Chris"
                              :user/age  44}
                             {:db/id (d/tempid :db.part/user)
                              :user/name "Bob"}])
      
      ;; Wait a bit for it to replicate
      (Thread/sleep 500)

      ;; We should now have two people with names in the destination database
      (let [names (d/q `[:find ?n
                          :where [?e :user/name ?n]]
                        (d/db c2))]
        (is (= #{"Chris" "Bob"} (set (map first names)))))

      
      (finally
        (rep/stop replicator)))))


(deftest test-replication-with-partitions
  ;; Partitions
  (with-databases
    ;; Start the replication
    (rep/start replicator)

    (try
      ;; Create an attribute in the source database
      (define-attr c1 :user/name :string)
      (define-attr c1 :user/age :long)

      ;; Create a partition
      @(d/transact c1 [{:db/id                 (d/tempid :db.part/db)
                              :db/ident              :db.part/person
                              :db.install/_partition :db.part/db}])

      ;; Then put some people in the new partition
      @(d/transact c1 [{:db/id     (d/tempid :db.part/person)
                              :user/name "Chris"
                              :user/age  44}
                             {:db/id     (d/tempid :db.part/person)
                              :user/name "Bob"}])
      
      ;; Wait a bit for it to replicate
      (Thread/sleep 500)

      ;; We should now have two people with names in the destination database
      (let [people (d/q `[:find ?n ?e
                          :where [?e :user/name ?n]]
                        (d/db c2))]
        (is (= #{"Chris" "Bob"} (set (map first people))))
        (is (= #{:db.part/person} (->> people
                                       (map second)
                                       (map d/part)
                                       (map (partial d/entity (d/db c2)))
                                       (map :db/ident)
                                       set))))

      
      (finally
        (rep/stop replicator)))))
