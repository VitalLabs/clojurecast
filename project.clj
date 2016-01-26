(defproject org.clojurecast/clojurecast "0.1.2"
  :description ""
  :url ""
  :license {:name "Proprietary"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.cache "0.6.4"]
                 [com.hazelcast/hazelcast-all "3.5.3"]
                 [javax.cache/cache-api "1.0.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/nippy "2.10.0"]
                 [clj-time "0.11.0"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/test.check "0.8.1"]]
                   :source-paths ["dev"]
                   :jvm-opts ^:replace ["-server"]}})
