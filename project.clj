(defproject org.clojurecast/clojurecast "0.1.3"
  :description ""
  :url ""
  :license {:name "Proprietary"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.nrepl "0.2.12"]                 
                 [com.hazelcast/hazelcast-all "3.6.1"]
                 [javax.cache/cache-api "1.0.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/nippy "2.11.1"]
                 [cider/cider-nrepl "0.12.0-SNAPSHOT"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/test.check "0.8.1"]]
                   :source-paths ["dev"]
                   :jvm-opts ^:replace ["-server"]}})
