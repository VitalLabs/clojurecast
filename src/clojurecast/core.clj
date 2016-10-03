;; Copyright (c) Vital Labs, Inc. All rights reserved.  The use and
;; distribution terms for this software are covered by the MIT
;; License (https://opensource.org/licenses/MIT) which can be found
;; in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this notice,
;; or any other, from this software.

(ns clojurecast.core
  (:refer-clojure :exclude [send agent-error shutdown-agents restart-agent agent
                            await-for atom])
  (:require [clojurecast.component :as com]
            [clojurecast.lang.agent :as agent]
            [clojurecast.lang.atom :as atom]
            [clojurecast.lang.cache :as cache]
            [clojurecast.lang.buffers :as buffers])
  (:import [com.hazelcast.core Hazelcast HazelcastInstance]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(def ^:dynamic ^HazelcastInstance *instance* nil)

(defrecord Node [^HazelcastInstance instance]
  com/Component
  (initialized? [_] true)
  (started? [_] (boolean instance))
  (migrate? [_] false)
  (-init [this] this)
  (-start [this]
    (let [instance (Hazelcast/newHazelcastInstance)]
      (if (thread-bound? #'*instance*)
        (set! *instance* instance)
        (.bindRoot #'*instance* instance))
      (assoc this :instance instance)))
  (-stop [this]
    (when-not (.isClusterSafe (.getPartitionService instance))
      (.forceLocalMemberToBeSafe (.getPartitionService instance)
                                 1 
                                 TimeUnit/HOURS))
    (.shutdown instance)
    (if (thread-bound? #'*instance*)
      (set! *instance* nil)
      (.bindRoot #'*instance* nil))
    (assoc this :instance nil))
  (-migrate [this] this))

(defn ^com.hazelcast.core.IAtomicLong atomic-long
  ([] (atomic-long "default"))
  ([name]
   {:pre [*instance*]}
   (.getAtomicLong *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getAtomicLong instance (str name))))

(defn ^com.hazelcast.core.IAtomicReference atomic-reference
  ([] (atomic-reference "default"))
  ([name]
   {:pre [*instance*]}
   (.getAtomicReference *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getAtomicReference instance (str name))))

(defn ^com.hazelcast.core.ClientService client-service
  ([]
   {:pre [*instance*]}
   (.getClientService *instance*))
  ([^HazelcastInstance instance]
   (.getClientService instance)))

(defn ^com.hazelcast.core.Cluster cluster
  ([]
   {:pre [*instance*]}
   (.getCluster *instance*))
  ([^HazelcastInstance instance]
   (.getCluster instance)))

(defn ^com.hazelcast.config.Config config
  ([]
   {:pre [*instance*]}
   (.getConfig *instance*))
  ([^HazelcastInstance instance]
   (.getConfig instance)))

(defn ^com.hazelcast.core.ICountDownLatch count-down-latch
  ([] (count-down-latch "default"))
  ([name]
   {:pre [*instance*]}
   (.getCountDownLatch *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getCountDownLatch instance (str name))))

(defn ^java.util.Collection distributed-objects
  ([]
   {:pre [*instance*]}
   (.getDistributedObjects *instance*))
  ([^HazelcastInstance instance]
   (.getDistributedObjects instance)))

(defn ^com.hazelcast.core.IExecutorService executor-service
  ([] (executor-service "default"))
  ([name]
   {:pre [*instance*]}
   (.getExecutorService *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getExecutorService instance (str name))))

(defn ^com.hazelcast.core.IdGenerator id-generator
  ([] (id-generator "default"))
  ([name]
   {:pre [*instance*]}
   (.getIdGenerator *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getIdGenerator instance (str name))))

(defn ^com.hazelcast.mapreduce.JobTracker job-tracker
  ([] (job-tracker "default"))
  ([name]
   {:pre [*instance*]}
   (.getJobTracker *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getJobTracker instance (str name))))

(defn ^com.hazelcast.core.LifecycleService lifecycle-service
  ([]
   {:pre [*instance*]}
   (.getLifecycleService *instance*))
  ([^HazelcastInstance instance]
   (.getLifecycleService instance)))

(defn ^com.hazelcast.core.IList distributed-list
  ([] (distributed-list "default"))
  ([name]
   {:pre [*instance*]}
   (.getList *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getList instance (str name))))

(defn ^com.hazelcast.core.Endpoint local-endpoint
  ([]
   {:pre [*instance*]}
   (.getLocalEndpoint *instance*))
  ([^HazelcastInstance instance]
   (.getLocalEndpoint instance)))

(defn ^com.hazelcast.core.ILock lock
  ([] (lock "default"))
  ([name]
   {:pre [*instance*]}
   (.getLock *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getLock instance (str name))))

(defn ^com.hazelcast.logging.LoggingService logging-service
  ([]
   {:pre [*instance*]}
   (.getLoggingService *instance*))
  ([^HazelcastInstance instance]
   (.getLoggingService instance)))

(defn ^com.hazelcast.core.IMap distributed-map
  ([] (distributed-map "default"))
  ([name]
   {:pre [*instance*]}
   (.getMap *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getMap instance (str name))))

(defn ^com.hazelcast.core.MultiMap multi-map
  ([] (multi-map "default"))
  ([name]
   {:pre [*instance*]}
   (.getMultiMap *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getMultiMap instance (str name))))

(defn ^String instance-name
  ([]
   {:pre [*instance*]}
   (.getName *instance*))
  ([^HazelcastInstance instance]
   (.getName instance)))

(defn ^com.hazelcast.core.PartitionService partition-service
  ([]
   {:pre [*instance*]}
   (.getPartitionService *instance*))
  ([^HazelcastInstance instance]
   (.getPartitionService instance)))

(defn ^com.hazelcast.core.IQueue queue
  ([] (queue "default"))
  ([name]
   {:pre [*instance*]}
   (.getQueue *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getQueue instance (str name))))

(defn ^com.hazelcast.quorum.QuorumService quorum-service
  ([]
   {:pre [*instance*]}
   (.getQuorumService *instance*))
  ([^HazelcastInstance instance]
   (.getQuorumService instance)))

(defn ^com.hazelcast.core.ITopic reliable-topic
  ([] (reliable-topic "default"))
  ([name]
   {:pre [*instance*]}
   (.getReliableTopic *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getReliableTopic instance (str name))))

(defn ^com.hazelcast.core.ReplicatedMap replicated-map
  ([] (replicated-map "default"))
  ([name]
   {:pre [*instance*]}
   (.getReplicatedMap *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getReplicatedMap instance (str name))))

(defn ^com.hazelcast.ringbuffer.Ringbuffer ring-buffer
  ([] (ring-buffer "default"))
  ([name]
   {:pre [*instance*]}
   (.getRingbuffer *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getRingbuffer instance (str name))))

(defn ^com.hazelcast.core.ISemaphore semaphore
  ([] (semaphore "default"))
  ([name]
   {:pre [*instance*]}
   (.getSemaphore *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getSemaphore instance (str name))))

(defn ^com.hazelcast.core.ISet distributed-set
  ([] (distributed-set "default"))
  ([name]
   {:pre [*instance*]}
   (.getSet *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getSet instance (str name))))

(defn ^com.hazelcast.core.ITopic topic
  ([] (topic "default"))
  ([name]
   {:pre [*instance*]}
   (.getTopic *instance* (str name)))
  ([^HazelcastInstance instance name]
   (.getTopic instance (str name))))

(defn ^java.util.concurrent.ConcurrentMap user-context
  ([]
   {:pre [*instance*]}
   (.getUserContext *instance*))
  ([^HazelcastInstance instance]
   (.getUserContext instance)))

(defn clean-down
  "Destroys all distributed objects managed by Hazelcast. Use with caution."
  []
  (doseq [^com.hazelcast.core.DistributedObject object (distributed-objects)]
    (.destroy object)))

(defmacro with-distributed-objects
  "Runs the code in `body` with `object`, and destroys `object` at the end."
  [bindings & body]
  `(let ~bindings
     (try
       ~@body
       (finally
         ~@(for [binding (map first (partition 2 bindings))]
             `(.destroy ~binding))))))

(defn agent
  ([]
   (agent "default"))
  ([name]
   {:pre [*instance*]}
   (agent *instance* name))
  ([^HazelcastInstance instance name]
   (agent/->Agent (atomic-reference (str name "-agent-state"))
                  (executor-service instance "agent-send-pool")
                  (atomic-long instance (str name "-counter"))
                  (queue instance (str name "-action-queue"))
                  (atomic-reference instance (str name "-agent-error"))
                  (atomic-reference instance (str name "-error-mode"))
                  (atomic-reference instance (str name "-error-handler"))
                  (atomic-reference instance (str name "-agent-vf"))
                  (atomic-reference instance (str name "-agent-watches"))
                  (atomic-reference instance (str name "-agent-meta"))
                  (str name)
                  (ThreadLocal.))))

(defn send
  [^clojurecast.lang.agent.Agent agent f & args]
  (.dispatch agent f args))

(defn shutdown-agents
  [instance]
  (.shutdown (executor-service instance "agent-send-pool")))

(defn agent-error
  [^clojurecast.lang.agent.Agent agent]
  (.get ^com.hazelcast.core.IAtomicReference (.-error agent)))

(defn restart-agent
  [^clojurecast.lang.agent.Agent agent new-state & {:keys [clear-actions]}]
  (.restart agent new-state clear-actions))

(defn await-for
  [timeout-ms & agents]
  {:pre [*instance*]}
  (with-distributed-objects
    [latch (doto (count-down-latch *instance* (java.util.UUID/randomUUID))
             (.trySetCount (count agents)))]
    (let [count-down (fn [agent] (.countDown latch) agent)]
      (doseq [agent agents]
        (send agent count-down))
      (.await latch timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS))))

(defn atom
  ([]
   (atom "default"))
  ([name]
   {:pre [*instance*]}
   (atom *instance* name))
  ([^HazelcastInstance instance name]
   (atom/->Atom (.getAtomicReference instance (str name))
                (.getAtomicReference instance (str name "vf"))
                (.getAtomicReference instance (str name "watcher"))
                (.getAtomicReference instance (str name "meta")))))
