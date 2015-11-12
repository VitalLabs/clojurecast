(ns clojurecast.core
  (:refer-clojure :exclude [send agent-error shutdown-agents restart-agent agent
                            await-for])
  (:require [com.stuartsierra.component :as com]
            [clojurecast.lang.atom :as atom]
            [clojurecast.lang.cache :as cache]
            [clojurecast.lang.buffers :as buffers])
  (:import [com.hazelcast.core Hazelcast HazelcastInstance]
           [java.util.concurrent TimeUnit]))

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
        (when-not (.isClusterSafe (.getPartitionService instance))
          (.forceLocalMemberToBeSafe (.getPartitionService instance)
                                     10000
                                     TimeUnit/MILLISECONDS))
        (.shutdown instance)
        (if (thread-bound? #'*instance*)
          (set! *instance* nil)
          (.bindRoot #'*instance* nil))
        (assoc this :instance nil))
      this)))

(defn ^com.hazelcast.core.IAtomicLong atomic-long
  ([] (atomic-long "default"))
  ([name]
   {:pre [*instance*]}
   (.getAtomicLong *instance* (str name)))
  ([instance name]
   (.getAtomicLong instance (str name))))

(defn ^com.hazelcast.core.IAtomicReference atomic-reference
  ([] (atomic-reference "default"))
  ([name]
   {:pre [*instance*]}
   (.getAtomicReference *instance* (str name)))
  ([instance name]
   (.getAtomicReference instance (str name))))

(defn ^com.hazelcast.core.ClientService client-service
  ([]
   {:pre [*instance*]}
   (.getClientService *instance*))
  ([instance]
   (.getClientService instance)))

(defn ^com.hazelcast.core.Cluster cluster
  ([]
   {:pre [*instance*]}
   (.getCluster *instance*))
  ([instance]
   (.getCluster instance)))

(defn ^com.hazelcast.config.Config config
  ([]
   {:pre [*instance*]}
   (.getConfig *instance*))
  ([instance]
   (.getConfig instance)))

(defn ^com.hazelcast.core.ICountDownLatch count-down-latch
  ([] (count-down-latch "default"))
  ([name]
   {:pre [*instance*]}
   (.getCountDownLatch *instance* (str name)))
  ([instance name]
   (.getCountDownLatch instance (str name))))

(defn ^java.util.Collection distributed-objects
  ([]
   {:pre [*instance*]}
   (.getDistributedObjects *instance*))
  ([instance]
   (.getDistributedObjects instance)))

(defn ^com.hazelcast.core.IExecutorService executor-service
  ([] (executor-service "default"))
  ([name]
   {:pre [*instance*]}
   (.getExecutorService *instance* (str name)))
  ([instance name]
   (.getExecutorService instance (str name))))

(defn ^com.hazelcast.core.IdGenerator id-generator
  ([] (id-generator "default"))
  ([name]
   {:pre [*instance*]}
   (.getIdGenerator *instance* (str name)))
  ([instance name]
   (.getIdGenerator instance (str name))))

(defn ^com.hazelcast.mapreduce.JobTracker job-tracker
  ([] (job-tracker "default"))
  ([name]
   {:pre [*instance*]}
   (.getJobTracker *instance* (str name)))
  ([instance name]
   (.getJobTracker instance (str name))))

(defn ^com.hazelcast.core.LifecycleService lifecycle-service
  ([]
   {:pre [*instance*]}
   (.getLifecycleService *instance*))
  ([instance]
   (.getLifecycleService instance)))

(defn ^com.hazelcast.core.IList distributed-list
  ([] (distributed-list "default"))
  ([name]
   {:pre [*instance*]}
   (.getList *instance* (str name)))
  ([instance name]
   (.getList instance (str name))))

(defn ^com.hazelcast.core.Endpoint local-endpoint
  ([]
   {:pre [*instance*]}
   (.getLocalEndpoint *instance*))
  ([instance]
   (.getLocalEndpoint instance)))

(defn ^com.hazelcast.core.ILock lock
  ([] (lock "default"))
  ([name]
   {:pre [*instance*]}
   (.getLock *instance* (str name)))
  ([instance name]
   (.getLock instance (str name))))

(defn ^com.hazelcast.logging.LoggingService logging-service
  ([]
   {:pre [*instance*]}
   (.getLoggingService *instance*))
  ([instance]
   (.getLoggingService instance)))

(defn ^com.hazelcast.core.IMap distributed-map
  ([] (distributed-map "default"))
  ([name]
   {:pre [*instance*]}
   (.getMap *instance* (str name)))
  ([instance name]
   (.getMap instance (str name))))

(defn ^com.hazelcast.core.MultiMap multi-map
  ([] (multi-map "default"))
  ([name]
   {:pre [*instance*]}
   (.getMultiMap *instance* (str name)))
  ([instance name]
   (.getMultiMap instance (str name))))

(defn ^String instance-name
  ([]
   {:pre [*instance*]}
   (.getName *instance*))
  ([instance]
   (.getName instance)))

(defn ^com.hazelcast.core.PartitionService partition-service
  ([]
   {:pre [*instance*]}
   (.getPartitionService *instance*))
  ([instance]
   (.getPartitionService instance)))

(defn ^com.hazelcast.core.IQueue queue
  ([] (queue "default"))
  ([name]
   {:pre [*instance*]}
   (.getQueue *instance* (str name)))
  ([instance name]
   (.getQueue instance (str name))))

(defn ^com.hazelcast.quorum.QuorumService quorum-service
  ([]
   {:pre [*instance*]}
   (.getQuorumService *instance*))
  ([instance]
   (.getQuorumService instance)))

(defn ^com.hazelcast.core.ITopic reliable-topic
  ([] (reliable-topic "default"))
  ([name]
   {:pre [*instance*]}
   (.getReliableTopic *instance* (str name)))
  ([instance name]
   (.getReliableTopic instance (str name))))

(defn ^com.hazelcast.core.ReplicatedMap replicated-map
  ([] (replicated-map "default"))
  ([name]
   {:pre [*instance*]}
   (.getReplicatedMap *instance* (str name)))
  ([instance name]
   (.getReplicatedMap instance (str name))))

(defn ^com.hazelcast.ringbuffer.Ringbuffer ring-buffer
  ([] (ring-buffer "default"))
  ([name]
   {:pre [*instance*]}
   (.getRingbuffer *instance* (str name)))
  ([instance name]
   (.getRingbuffer instance (str name))))

(defn ^com.hazelcast.core.ISemaphore semaphore
  ([] (semaphore "default"))
  ([name]
   {:pre [*instance*]}
   (.getSemaphore *instance* (str name)))
  ([instance name]
   (.getSemaphore instance (str name))))

(defn ^com.hazelcast.core.ISet distributed-set
  ([] (distributed-set "default"))
  ([name]
   {:pre [*instance*]}
   (.getSet *instance* (str name)))
  ([instance name]
   (.getSet instance (str name))))

(defn ^com.hazelcast.core.ITopic topic
  ([] (topic "default"))
  ([name]
   {:pre [*instance*]}
   (.getTopic *instance* (str name)))
  ([instance name]
   (.getTopic instance (str name))))

(defn ^java.util.concurrent.ConcurrentMap user-context
  ([]
   {:pre [*instance*]}
   (.getUserContext *instance*))
  ([instance]
   (.getUserContext instance)))

(defn clean-down
  "Destroys all distributed objects managed by Hazelcast. Use with caution."
  []
  (doseq [^com.hazelcast.core.DistributedObject object (distributed-objects)]
    (.destroy object)))

