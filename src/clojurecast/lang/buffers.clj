;; Copyright (c) Vital Labs, Inc. All rights reserved.  The use and
;; distribution terms for this software are covered by the MIT
;; License (https://opensource.org/licenses/MIT) which can be found
;; in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this notice,
;; or any other, from this software.

(ns clojurecast.lang.buffers
  (:require [clojure.core.async.impl.protocols :as impl])
  (:import [java.util LinkedList Queue]
           [com.hazelcast.core HazelcastInstance IQueue IAtomicReference]))

(set! *warn-on-reflection* true)

(deftype FixedBuffer [^IQueue buf ^long n]
  impl/Buffer
  (full? [this]
    (>= (.size buf) n))
  (remove! [this]
    (.poll buf))
  (add!* [this itm]
    (.offer buf itm)
    this)
  (close-buf! [this])
  clojure.lang.Counted
  (count [this]
    (.size buf)))

(defn fixed-buffer
  [^HazelcastInstance instance ^String name ^long n]
  (FixedBuffer. (.getQueue instance name) n))

(deftype DroppingBuffer [^IQueue buf ^long n]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this]
    false)
  (remove! [this]
    (.poll buf))
  (add!* [this itm]
    (when-not (>= (.size buf) n)
      (.offer buf itm))
    this)
  (close-buf! [this])
  clojure.lang.Counted
  (count [this]
    (.size buf)))

(defn dropping-buffer
  [^HazelcastInstance instance ^String name ^long n]
  (DroppingBuffer. (.getQueue instance name) n))

(deftype SlidingBuffer [^IQueue buf ^long n]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this]
    false)
  (remove! [this]
    (.poll buf))
  (add!* [this itm]
    (when (= (.size buf) n)
      (impl/remove! this))
    (.offer buf itm)
    this)
  (close-buf! [this])
  clojure.lang.Counted
  (count [this]
    (.size buf)))

(defn sliding-buffer
  [^HazelcastInstance instance ^String name ^long n]
  (SlidingBuffer. (.getQueue instance name) n))

(defn- aref-exists? [^HazelcastInstance instance ^String name]
  (.getDistributedObject instance "hz:impl:atomicReferenceService" name))

(defn- undelivered? [^IAtomicReference aref]
  (identical? (.get aref) ::no-val))

(deftype PromiseBuffer [^IAtomicReference aref]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [_]
    false)
  (remove! [_]
    (.get aref))
  (add!* [this itm]
    (when (undelivered? aref)
      (.set aref itm))
    this)
  (close-buf! [_]
    (when (undelivered? aref)
      (.set aref nil)))
  clojure.lang.Counted
  (count [_]
    (if (undelivered? val)
      0
      1)))

(defn promise-buffer
  [^HazelcastInstance instance ^String name]
  (let [aref (.getAtomicReference instance name)]
    (when-not (aref-exists? aref)
      (.set aref ::no-val))
    (PromiseBuffer. aref)))
