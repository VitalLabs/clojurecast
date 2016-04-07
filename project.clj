(defproject org.clojurecast/clojurecast "0.1.3-SNAPSHOT"
  :description ""
  :url ""
  :license {:name "Proprietary"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.nrepl "0.2.12"]            
                 [com.hazelcast/hazelcast-all "3.6.2"]
                 [javax.cache/cache-api "1.0.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/nippy "2.11.1"]
                 [cider/cider-nrepl "0.12.0-SNAPSHOT"]
                 [com.datomic/datomic-pro "0.9.5350"
                  :exclusions [joda-time
                               org.clojure/tools.cli
                               org.slf4j/slf4j-nop
                               org.slf4j/log4j-over-slf4j
                               org.apache.httpcomponents/httpclient]]
                 [org.apache.logging.log4j/log4j-core "2.4.1"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.4.1"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/test.check "0.8.1"]]
                   :source-paths ["dev"]}}
  :repositories {"my.datomic.com" {:url "https://my.datomic.com/repo"
                                   :creds :gpg}})
