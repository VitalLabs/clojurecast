(ns clojurecast.services.hazelcast.atom
  (:require [clojurecast.services.hazelcast :as hz-svc]
            [clojurecast.services.hazelcast.core :as hz]
            [taoensso.nippy :as nippy]))

(defn- validate
  [^clojure.lang.IRef ref val]
  (let [f (.getValidator ref)]
    (try
      (when (and f (not (boolean (f val))))
        (throw (IllegalStateException. "Invalid reference state")))
      (catch RuntimeException e
        (throw e))
      (catch Exception e
        (throw (IllegalStateException. "Invalid reference state" e))))))

(defn- notify-watches
  [^clojure.lang.IRef ref oldval newval]
  (doseq [[k f] (.getWatches ref)]
    (f k ref oldval newval)))

(deftype HazelcastAtom [service
                        ^String state-name
                        ^String watches-name
                        ^String validator-name
                        ^String meta-name
                        nippy-options]
  hz-svc/HazelcastService
  (get-instance [ref]
    (hz-svc/get-instance service))
  (get-password [ref]
    (hz-svc/get-password service))
  
  clojure.lang.IRef
  (deref [ref]
    (let [val (.get (hz/get-atomic-reference ref state-name))]
      (when-not (nil? val)
        (nippy/thaw val nippy-options))))
  (setValidator [ref validator-fn]
    (.set (hz/get-atomic-reference ref validator-name) validator-fn))
  (getValidator [ref]
    (.get (hz/get-atomic-reference ref validator-name)))
  (getWatches [ref]
    (.get (hz/get-atomic-reference ref watches-name)))
  (addWatch [ref key fn]
    (.set (hz/get-atomic-reference ref watches-name)
          (assoc (.getWatches ref) key fn)))
  (removeWatch [ref key]
    (.set (hz/get-atomic-reference ref watches-name)
          (dissoc (.getWatches ref) key)))

  clojure.lang.IReference
  (meta [ref]
    (.get (hz/get-atomic-reference ref meta-name)))
  (alterMeta [ref f args]
    (.set (hz/get-atomic-reference ref meta-name)
          (apply f (.meta ref) args)))
  (resetMeta [ref meta]
    (.set (hz/get-atomic-reference ref meta-name) meta))

  clojure.lang.IAtom
  (swap [ref f]
    (let [oldval (.deref ref)
          newval (f oldval)]
      (if (.compareAndSet ref oldval newval)
        newval
        (recur f))))
  (swap [ref f x]
    (let [oldval (.deref ref)
          newval (f oldval x)]
      (if (.compareAndSet ref oldval newval)
        newval
        (recur f x))))
  (swap [ref f x y]
    (let [oldval (.deref ref)
          newval (f oldval x y)]
      (if (.compareAndSet ref oldval newval)
        newval
        (recur f x y))))
  (swap [ref f x y args]
    (let [oldval (.deref ref)
          newval (apply f oldval x y args)]
      (if (.compareAndSet ref oldval newval)
        newval
        (recur f x y args))))
  (compareAndSet [ref oldval newval]
    (validate ref newval)
    (if (.compareAndSet (hz/get-atomic-reference ref state-name)
                        (nippy/freeze oldval nippy-options)
                        (nippy/freeze newval nippy-options))
      (do
        (notify-watches ref oldval newval)
        true)
      false))
  (reset [ref newval]
    (let [oldval (.deref ref)]
      (validate ref newval)
      (.set (hz/get-atomic-reference ref state-name)
            (nippy/freeze newval nippy-options))
      (notify-watches ref oldval newval)
      newval)))

(defn get-atom
  ([service name]
   (get-atom service name {}))
  ([service name nippy-options]
   (HazelcastAtom. service
                   name
                   (str name "/watches")
                   (str name "/validator")
                   (str name "/meta")
                   (merge {:password (hz-svc/get-password service)}
                          nippy-options))))
