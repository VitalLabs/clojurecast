(ns clojurecast.core
  (:require [com.stuartsierra.component :as com])
  (:import [com.hazelcast.core Hazelcast HazelcastInstance]))

(def ^:dynamic ^HazelcastInstance *instance*)

(defrecord Node [^HazelcastInstance instance]
  com/Lifecycle
  (start [this]
    (if instance
      this
      (let [instance (Hazelcast/newHazelcastInstance)]
        (if (thread-bound? #'*instance*)
          (set! *instance* instance)
          (.bindRoot #'*instance* instance))
        (assoc this :instance instance))))
  (stop [this]
    (if instance
      (do
        (.shutdown instance)
        (if (thread-bound? #'*instance*)
          (set! *instance* nil)
          (.bindRoot #'*instance* nil))
        (assoc this :instance nil))
      this)))

(defn ^com.hazelcast.core.IAtomicLong atomic-long
  [^String name]
  {:pre [*instance*]}
  (.getAtomicLong *instance* name))

(defn ^com.hazelcast.core.ClientService client-service
  []
  {:pre [*instance*]}
  (.getClientService *instance*))

(defn ^com.hazelcast.core.Cluster cluster
  []
  {:pre [*instance*]}
  (.getCluster *instance*))

(defn ^com.hazelcast.config.Config config
  []
  {:pre [*instance*]}
  (.getConfig *instance*))
