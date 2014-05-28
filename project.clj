(defproject datomic-replication "0.1.0-SNAPSHOT"
  
  :description "Datomic Replication"
  :url "http://github.com/RallySoftware/datomic-replication"
  
  :license "MIT License"
  
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.datomic/datomic-free "0.9.4755" :exclusions [org.slf4j/slf4j-nop org.slf4j/slf4j-log4j12]]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [enos "0.1.2"]])
