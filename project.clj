(defproject com.rallydev/datomic-replication "0.1.1-SNAPSHOT"
  
  :description "Datomic Replication"
  :url "http://github.com/RallySoftware/datomic-replication"
  
  :license "MIT License"
  
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.1.2"]]

  :jvm-opts ["-Djava.awt.headless=true"]
  
  :profiles
  {:dev {:dependencies [[com.datomic/datomic-free "0.9.4766"]]}})
