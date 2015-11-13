(ns clojurecast.lang.atom
  (:require [clojurecast.lang.util :as util]
            [clojurecast.lang.interfaces])
  (:import [com.hazelcast.core HazelcastInstance IAtomicReference]
           [clojurecast.lang.interfaces IValidate IWatchable]))

(deftype Atom [^IAtomicReference state
               ^IAtomicReference validator
               ^IAtomicReference watches
               ^IAtomicReference meta]
  clojure.lang.IAtom
  (swap [this f]
    (let [oldval (.deref this)
          newval (f oldval)]
      (if (.compareAndSet this oldval newval)
        newval
        (recur f))))
  (swap [this f x]
    (let [oldval (.deref this)
          newval (f oldval x)]
      (if (.compareAndSet this oldval newval)
        newval
        (recur f x))))
  (swap [this f x y]
    (let [oldval (.deref this)
          newval (f oldval x y)]
      (if (.compareAndSet this oldval newval)
        newval
        (recur f x y))))
  (swap [this f x y args]
    (let [oldval (.deref this)
          newval (apply f oldval x y args)]
      (if (.compareAndSet this oldval newval)
        newval
        (recur f x y args))))
  (compareAndSet [this oldval newval]
    (.validate this newval)
    (when-let [ret (.compareAndSet state
                                   (util/freeze oldval)
                                   (util/freeze newval))]
      (.notifyWatches this oldval newval)
      ret))
  (reset [this newval]
    (let [oldval (.deref this)]
      (.validate this newval)
      (.set state (util/freeze newval))
      (.notifyWatches this oldval newval)
      newval))

  clojure.lang.IRef
  (deref [_]
    (util/thaw (.get state)))
  (setValidator [this f]
    (.validate this f (.deref this))
    (.set validator f))
  (getValidator [_] (.get validator))
  (getWatches [_] (.get watches))
  (addWatch [this k f] (.set watches (.assoc (.get watches) k f)))
  (removeWatch [this k] (.set watches (.without (.get watches) k)))

  IWatchable
  (notifyWatches [this oldval newval]
    (doseq [[k f] (.get watches)]
      (f k this oldval newval)))

  clojure.lang.IReference
  (meta [_]
    (util/thaw (.get meta)))
  (alterMeta [this f args]
    (.set meta (util/freeze (apply f (.meta this) args))))
  (resetMeta [_ m]
    (.set meta (util/freeze m)))

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
