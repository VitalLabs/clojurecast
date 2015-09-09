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
  ([] (atomic-long "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getAtomicLong *instance* name)))

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

(defn ^com.hazelcast.core.ICountDownLatch count-down-latch
  ([] (count-down-latch "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getCountDownLatch *instance* name)))

(defn ^java.util.Collection distributed-objects
  []
  {:pre [*instance*]}
  (.getDistributedObjects *instance*))

(defn ^com.hazelcast.core.IExecutorService executor-service
  ([] (executor-service "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getExecutorService *instance* name)))

(defn ^com.hazelcast.core.IdGenerator id-generator
  ([] (id-generator "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getIdGenerator *instance* name)))

(defn ^com.hazelcast.mapreduce.JobTracker job-tracker
  ([] (job-tracker "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getJobTracker *instance* name)))
