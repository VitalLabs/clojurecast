;; Copyright (c) Vital Labs, Inc. All rights reserved.  The use and
;; distribution terms for this software are covered by the MIT
;; License (https://opensource.org/licenses/MIT) which can be found
;; in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this notice,
;; or any other, from this software.

(ns clojurecast.lang.agent
  (:require [clojurecast.lang.util :as util]
            [clojurecast.lang.interfaces])
  (:import [clojurecast.lang.interfaces IAgent IValidate IWatchable]
           [java.util.concurrent Executor]
           [com.hazelcast.core HazelcastInstance IAtomicReference IMap IQueue]
           [com.hazelcast.core IAtomicLong IExecutorService]))

(declare make-action)

(deftype Agent [^IAtomicReference state
                ^IExecutorService exec
                ^IAtomicLong send-off-thread-pool-counter
                ^IQueue action-queue
                ^IAtomicReference error
                ^IAtomicReference error-mode
                ^IAtomicReference error-handler
                ^IAtomicReference validator
                ^IAtomicReference watches
                ^IAtomicReference meta
                ^String name
                ^ThreadLocal nested]
  IAgent
  (dispatch [this f args]
    (.dispatch this f args exec))
  (dispatch [this f args exec]
    (when-let [err (.get error)]
      (throw (ex-info "Agent is failed, needs restart" {:error err})))
    (let [action (make-action name f args)]
      (if-let [actions (.get nested)]
        (.set nested (conj actions action))
        (.enqueue this action)))
    this)
  (doRun [this f args]
    (try
      (.set nested [])
      (try
        (let [oldval (.get state)
              newval (apply f oldval args)]
          (.setState this newval)
          (.notifyWatches this oldval newval))
        (catch Throwable e
          (.set error e)))
      (if (.get error)
        (do (.set nested nil)
            (try
              (when-let [f (.get error-handler)]
                (f this (.get error)))
              (catch Throwable e))
            (when (identical? (.get error-mode) :continue)
              (.set error nil)))
        (.releasePendingSends this))
      (when-let [next-action (and (nil? (.get error)) (.peek action-queue))]
        (.execute this next-action))
      (finally
        (.set nested nil))))
  (setState [this newval]
    (.validate this newval)
    (let [ret (not= (.get state) newval)]
      (when ret
        (.set state newval))
      ret))
  (setErrorMode [_ k]
    (.set error-mode k))
  (getErrorMode [_]
    error-mode)
  (setErrorHandler [_ f]
    (.set error-handler f))
  (getErrorHandler [_]
    error-handler)
  (restart [this new-state clear-actions?]
    (when (nil? (.get error))
      (throw (ex-info "Agent does not need a restart" {:agent this})))
    (.validate this new-state)
    (.set state new-state)
    (if clear-actions?
      (doseq [action action-queue]
        (.remove action-queue action))
      (when-let [prior-action (.peek action-queue)]
        (.execute this prior-action))))
  (releasePendingSends [this]
    (if-let [sends (.get nested)]
      (do (doseq [action sends]
            (.enqueue this action))
          (.set nested [])
          (count sends))
      0))
  (execute [this action]
    (try
      (.execute exec action)
      (catch Throwable e
        (if-let [f (.get error-handler)]
          (try
            (f this e)
            (catch Throwable e))))))
  (enqueue [this action]
    (let [prior-action (.peek action-queue)]
      (if (and (nil? prior-action) (nil? (.get error)))
        (.execute this action)
        (.offer action-queue action))))
  
  clojure.lang.IRef
  (deref [_]
    (.get state))
  (setValidator [this f]
    (.validate this f (.get state))
    (.set validator f))
  (getValidator [_]
    (.get validator))
  (getWatches [_]
    (.get watches))
  (addWatch [_ k f]
    (.set watches (.assoc (.get watches) k f)))
  (removeWatch [_ k]
    (.set watches (.without (.get watches) k)))

  IWatchable
  (notifyWatches [this oldval newval]
    (doseq [[k f] (.get watches)]
      (f k this oldval newval)))

  clojure.lang.IReference
  (meta [_]
    (.get meta))
  (alterMeta [_ f args]
    (.set meta (apply f (.get meta) args)))
  (resetMeta [_ m]
    (.set meta m))
  IValidate
  (validate [this val]
    (.validate this (.get validator) val))
  (validate [_ f val]
    (try
      (when (and (not (nil? f)) (false? (boolean (f val))))
        (throw (IllegalStateException. "Invalid reference state")))
      (catch RuntimeException e
        (throw e))
      (catch Exception e
        (throw (IllegalStateException. "Invalid reference state" e))))))

(defn- ^Runnable make-action
  [name f args]
  (fn []
    (let [agent ((resolve 'clojurecast.core/agent) name)]
      (.doRun ^clojurecast.lang.agent.Agent agent f args))))
