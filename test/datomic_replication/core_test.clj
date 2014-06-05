(ns datomic-replication.core-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [datomic-replication.core :as rep]))



(defmacro with-databases [& body]
  `(let [~'uri1 "datomic:free://localhost:4334/source"
         ~'uri2 "datomic:free://localhost:4334/dest"]
     (try
       (d/create-database ~'uri1)
       (d/create-database ~'uri2)
       (let [~'source-conn (d/connect ~'uri1)
             ~'dest-conn   (d/connect ~'uri2)
             stop#         (rep/replicate ~'source-conn ~'dest-conn)]
         (try
           ~@body
           (finally (stop#))))
       (finally
         (d/delete-database ~'uri1)
         (d/delete-database ~'uri2)))))


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
    ;; Create an attribute in the source database
    (define-attr source-conn :user/name :string)

    ;; Wait a bit for it to replicate
    (Thread/sleep 500)

    ;; And make sure that the attribute got replicated
    (is (= 1 (count (seq (d/datoms (d/db dest-conn) :avet :db/ident :user/name)))))))

(deftest test-replication-2
  (with-databases
    (define-attr source-conn :user/name :string)
    (define-attr source-conn :user/age :long)

    @(d/transact source-conn [{:db/id (d/tempid :db.part/user)
                               :user/name "Chris"
                               :user/age  44}
                              {:db/id (d/tempid :db.part/user)
                               :user/name "Bob"}])

    ;; Wait a bit for it to replicate
    (Thread/sleep 500)

    ;; We should now have two people with names in the destination database
    (let [names (d/q `[:find ?n
                       :where [?e :user/name ?n]]
                     (d/db dest-conn))]
      (is (= #{"Chris" "Bob"} (set (map first names)))))))


(deftest test-replication-with-partitions
  ;; Partitions
  (with-databases

    ;; Create an attribute in the source database
    (define-attr source-conn :user/name :string)
    (define-attr source-conn :user/age :long)

    ;; Create a partition
    @(d/transact source-conn [{:db/id                 (d/tempid :db.part/db)
                               :db/ident              :db.part/person
                               :db.install/_partition :db.part/db}])

    ;; Then put some people in the new partition
    @(d/transact source-conn [{:db/id     (d/tempid :db.part/person)
                               :user/name "Chris"
                               :user/age  44}
                              {:db/id     (d/tempid :db.part/person)
                               :user/name "Bob"}])

    ;; Wait a bit for it to replicate
    (Thread/sleep 500)

    ;; We should now have two people with names in the destination database
    (let [people (d/q `[:find ?n ?e
                        :where [?e :user/name ?n]]
                      (d/db dest-conn))]
      (is (= #{"Chris" "Bob"} (set (map first people))))
      (is (= #{:db.part/person} (->> people
                                     (map second)
                                     (map d/part)
                                     (map (partial d/entity (d/db dest-conn)))
                                     (map :db/ident)
                                     set))))))
