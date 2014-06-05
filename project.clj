(defproject com.rallydev/datomic-replication "0.1.1-SNAPSHOT"
  
  :description "Datomic Replication"
  :url "http://github.com/RallySoftware/datomic-replication"
  
  :license "MIT License"
  
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [org.clojure/tools.logging "0.1.2"]]

  :profiles
  {:dev {:dependencies [[com.datomic/datomic-free "0.9.4766"]]}})
