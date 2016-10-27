;; Copyright (c) Vital Labs, Inc. All rights reserved.  The use and
;; distribution terms for this software are covered by the MIT
;; License (https://opensource.org/licenses/MIT) which can be found
;; in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this notice,
;; or any other, from this software.

(ns clojurecast.stream
  "Provide a proper abstraction over Hazelcast's messaging interfaces
   via the excellent Manifold library.  Provides an async style interface
   for system messaging"
  (:require [clojure.tools.logging :as log]
            [clojurecast.core :as cc]
            [manifold.stream :as s]
            [manifold.stream.core :as stream]
            [manifold.stream.graph :as g]
            [manifold.bus :as bus]
            [manifold.deferred :as d]
            [manifold
             [executor :as exec]
             [utils :as utils]])
  (:import  [com.hazelcast.core ITopic IQueue MessageListener] 
            [com.hazelcast.topic ReliableMessageListener]
            [java.util.concurrent.atomic AtomicReference]))

;; NOTES:
;;
;; Topic RingBuffers must be configured
;; with a TTL and size that matches the max
;; expected recovery period over which a message
;; may end up being replayed on fail over. (1-2 mins?)
;;
;; AND/OR
;;
;; On an attach error (the ring buffer has timed out)
;; we may need to be able to replay events directly
;; from Cassandra's event log
;;


(def default-thread-count 4)

(defn- make-executor
  [options executor]
  (or executor
      (-> (or (:initial-threads options)
              default-thread-count)
          (exec/fixed-thread-executor))))

(def executor (make-executor {:initial-threads 2} nil))

(defn- topic-listener
  [topic stream]
  (reify MessageListener
    (onMessage [_ message]
      ;; TODO: Error handling?
      (->> (.getMessageObject message)
           (s/put! stream)
           (deref)))))

(deftype ReliableTopicListener
    [stream
     start
     ^:volatile-mutable last-seq
     sequence-handler]
  ReliableMessageListener
  (onMessage [_ message]
    ;; TODO: Error handling?
    (->> (.getMessageObject message)
         (sequence-handler last-seq)
         (s/put! stream)
         (deref))
    nil)
  (isLossTolerant [_]
    false)
  (isTerminal [_ failure]
    ;; TODO: What to do when onMessage fails?
    false)
  (retrieveInitialSequence [_] start)
  (storeSequence [_ seq]
    (set! last-seq seq)
    nil))

(defn reliable-topic-listener
  "Return a reliable topic listener"
  ([stream start]
   (reliable-topic-listener start identity))
  ([stream start seq-handler]
   (ReliableTopicListener. stream start nil seq-handler)))

(stream/def-source ReliableTopicSource
  [^com.hazelcast.core.ITopic topic
   ^:volatile-mutable stream
   ^:volatile-mutable listener]

  (isSynchronous [_] false)

  (description [this]
    {:source? true
     :drained? (s/drained? stream)
     :type "hazelcast.topic"})

  (close [this]
    (when listener
      (.removeMessageListener topic listener)
      (set! listener nil))
    (.markDrained this)
    (when-not (s/closed? stream)
      (.close stream)))

  (take [this default-val blocking?]
    (.take stream default-val blocking?))

  (take [this default-val blocking? timeout timeout-val]
    (.take stream default-val blocking? timeout timeout-val)))

(defn reliable-topic-stream
  ([topic-name]
   (reliable-topic-stream topic-name -1 identity))
  ([topic-name start]
   (reliable-topic-stream topic-name start identity))
  ([topic-name start handler]
   (let [stream (->> (s/stream 0)
                     (s/->source )
                     (s/onto executor))
         listener (reliable-topic-listener stream start handler)
         topic (cc/reliable-topic topic-name)
         listener-id (.addMessageListener topic listener)]
     (->ReliableTopicSource topic stream listener-id))))


;; NOTE: Defaults don't work due to need for start and handler
;; 
;; (extend-protocol s/Sourceable
;;
;;   com.hazelcast.topic.impl.reliable.ReliableTopicProxy
;;   (to-source [topic]
;;     (let [stream (s/stream)
;;           listener (reliable-topic-listener topic stream)]
;;       (.addMessageListener topic listener)
;;       (->ReliableTopicSource
;;        topic
;;        stream
;;        listener))))


;; ============= SINK ===============

(stream/def-sink ReliableTopicSink
  [^com.hazelcast.core.ITopic topic
   ^AtomicReference last-put]
  
  (isSynchronous [_] false)

  (description [this]
    {:source? true
     :drained? (s/closed? this)
     :type "hazelcast.topic"})

  (close [this]
    (.markClosed this)
    true)

  (put [this msg blocking?]
    (let [d (d/deferred)]
      (try
        (do (.publish topic msg)
            (d/success! d true))
        (catch java.lang.Throwable t
          (d/success! d false)))
      d))

  (put [this msg blocking? timeout timeout-val]
    (let [d (d/deferred)]
      (try
        (do (.publish topic msg)
            (d/success! d true))
        (catch java.lang.Throwable t
          (d/success! d false)))
      d)))

(extend-protocol stream/Sinkable

  com.hazelcast.topic.impl.reliable.ReliableTopicProxy
  (to-sink [topic]
    (->ReliableTopicSink 
      topic
      (AtomicReference. (d/success-deferred true)))))

;; ================== TOPIC BUS ========================

;; TODO: Distributed metadata service and API to topics or purely local construct?

(defprotocol IReliableEventBus
  (restore-subscription [topic start]))

(defn get-topic
  [instance topic-streams topic-name]
  ;; TODO: Cache the sink proxy
  (s/->sink (cc/reliable-topic instance topic-name)))

(deftype HazelcastReliableEventBus [instance config topic-streams]
  manifold.bus.IEventBus
  (subscribe [this topic]
    (reliable-topic-stream topic -1 (:handler config)))
  (publish [this topic message]
    (s/put! (get-topic instance topic-streams topic) message))
  ;; TODO - Improve introspection!
  (isActive [_ topic] true)
  (snapshot [_] nil)
  (downstream [_ topic] [])
  IReliableEventBus
  (restore-subscription [this topic start]
    ;; TODO: Check for existing subscription?
    (reliable-topic-stream topic start (:handler config))))

(defn reliable-event-bus
  [config]
  (->HazelcastReliableEventBus config))

