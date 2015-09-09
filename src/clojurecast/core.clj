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

(defn ^com.hazelcast.core.IAtomicReference atomic-reference
  ([] (atomic-reference "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getAtomicReference *instance* name)))

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

(defn ^com.hazelcast.core.LifecycleService lifecycle-service
  []
  {:pre [*instance*]}
  (.getLifecycleService *instance*))

(defn ^com.hazelcast.core.IList distributed-list
  ([] (distributed-list "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getList *instance* name)))

(defn ^com.hazelcast.core.Endpoint local-endpoint
  []
  {:pre [*instance*]}
  (.getLocalEndpoint *instance*))

(defn ^com.hazelcast.core.ILock lock
  ([] (lock "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getLock *instance* name)))

(defn ^com.hazelcast.logging.LoggingService logging-service
  []
  {:pre [*instance*]}
  (.getLoggingService *instance*))

(defn ^com.hazelcast.core.IMap distributed-map
  ([] (distributed-map "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getMap *instance* name)))

(defn ^com.hazelcast.core.MultiMap multi-map
  ([] (multi-map "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getMultiMap *instance* name)))

(defn ^String instance-name
  []
  {:pre [*instance*]}
  (.getName *instance*))

(defn ^com.hazelcast.core.PartitionService partition-service
  []
  {:pre [*instance*]}
  (.getPartitionService *instance*))

(defn ^com.hazelcast.core.IQueue queue
  ([] (queue "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getQueue *instance* name)))

(defn ^com.hazelcast.quorum.QuorumService quorum-service
  []
  {:pre [*instance*]}
  (.getQuorumService *instance*))

(defn ^com.hazelcast.core.ITopic reliable-topic
  ([] (reliable-topic "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getReliableTopic *instance* name)))

(defn ^com.hazelcast.core.ReplicatedMap replicated-map
  ([] (replicated-map "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getReplicatedMap *instance* name)))

(defn ^com.hazelcast.ringbuffer.Ringbuffer ring-buffer
  ([] (ring-buffer "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getRingbuffer *instance* name)))

(defn ^com.hazelcast.core.ISemaphore semaphore
  ([] (semaphore "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getSemaphore *instance* name)))

(defn ^com.hazelcast.core.ISet distributed-set
  ([] (distributed-set "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getSet *instance* name)))

(defn ^com.hazelcast.core.ITopic topic
  ([] (topic "default"))
  ([^String name]
   {:pre [*instance*]}
   (.getTopic *instance* name)))

(defn ^java.util.concurrent.ConcurrentMap user-context
  []
  {:pre [*instance*]}
  (.getUserContext *instance*))

(defn clean-down
  "Destroys all distributed objects managed by Hazelcast. Use with caution."
  []
  (doseq [^com.hazelcast.core.DistributedObject object (distributed-objects)]
    (.destroy object)))
