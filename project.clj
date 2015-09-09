(defproject clojurecast "0.1.0-SNAPSHOT"
  :description ""
  :url ""
  :license {:name "Proprietary"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.hazelcast/hazelcast-all "3.5.2"]
                 [javax.cache/cache-api "1.0.0"]
                 [com.stuartsierra/component "0.2.3"]
                 [clj-time "0.11.0"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/test.check "0.8.1"]]
                   :source-paths ["dev"]
                   :jvm-opts ^:replace ["-server"]}})
