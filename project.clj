(defproject org.clojurecast/clojurecast "0.1.3"
  :description "A Clojure infrastructure library for building distributed systems 
                built on the HazelCast library."
  :url "https://github.com/VitalLabs/clojurecast"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/tools.logging "0.3.1"]
                 [javax.cache/cache-api "1.0.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [manifold "0.1.5"]
                 [com.taoensso/nippy "2.12.1"]
                 [com.hazelcast/hazelcast-all "3.6.1"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.11"]
                                  [org.clojure/test.check "0.9.0"]]
                   :source-paths ["dev"]
                   :jvm-opts ^:replace ["-server"]}})
