(ns clojurecast.services.hazelcast.core
  (:require [clojurecast.services.hazelcast :as hz]
            [taoensso.nippy :as nippy])
  (:import [com.hazelcast.core
            
            Hazelcast
            HazelcastInstance

            Cluster
            DistributedObject
            Endpoint
            
            IAtomicLong
            IAtomicReference
            ICountDownLatch
            IExecutorService
            IQueue
            IList
            ILock
            IMap
            ISemaphore
            ISet
            ITopic
            MultiMap
            IdGenerator
            ReplicatedMap
            
            ClientService            
            LifecycleService
            PartitionService]
           [com.hazelcast.ringbuffer Ringbuffer]))

(defn ^IAtomicLong get-atomic-long
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getAtomicLong (hz/get-instance service) name))

(defn ^IAtomicReference get-atomic-reference
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getAtomicReference (hz/get-instance service) name))

(defn ^Cluster get-cluster
  [service]
  {:pre [(hz/get-instance service)]}
  (.getCluster (hz/get-instance service)))

(defn ^ICountDownLatch get-count-down-latch
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getCountDownLatch (hz/get-instance service) name))

(defn ^IExecutorService get-executor-service
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getExecutorService (hz/get-instance service) name))

(defn ^IdGenerator get-id-generator
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getIdGenerator (hz/get-instance service) name))

(defn ^LifecycleService get-lifecycle-service
  [service]
  {:pre [(hz/get-instance service)]}
  (.getLifecycleService (hz/get-instance service)))

(defn ^IList get-list
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getList (hz/get-instance service) name))

(defn ^Endpoint get-endpoint
  [service]
  {:pre [(hz/get-instance service)]}
  (.getLocalEndpoint (hz/get-instance service)))

(defn ^ILock get-lock
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getLock (hz/get-instance service) name))

(defn ^IMap get-map
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getMap (hz/get-instance service) name))

(defn ^MultiMap get-multi-map
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getMultiMap (hz/get-instance service) name))

(defn ^IQueue get-queue
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getQueue (hz/get-instance service) name))

(defn ^ITopic get-reliable-topic
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getReliableTopic (hz/get-instance service) name))

(defn ^ReplicatedMap get-replicated-map
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getReplicatedMap (hz/get-instance service) name))

(defn ^Ringbuffer get-ringbuffer
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getRingbuffer (hz/get-instance service) name))

(defn ^ISemaphore get-semaphore
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getSemaphore (hz/get-instance service) name))

(defn ^ISet get-set
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getSet (hz/get-instance service) name))

(defn ^ITopic get-topic
  [service ^String name]
  {:pre [(hz/get-instance service)]}
  (.getTopic (hz/get-instance service) name))
