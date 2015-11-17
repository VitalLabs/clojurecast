(ns clojurecast.lang.interfaces)

(definterface IValidate
  (^void validate [val])
  (^void validate [^clojure.lang.IFn f val]))

(definterface IWatchable
  (^void notifyWatches [oldval newval]))

(definterface IAgent
  (dispatch [^clojure.lang.IFn f
             ^clojure.lang.ISeq args])
  (dispatch [^clojure.lang.IFn f
             ^clojure.lang.ISeq args
             ^java.util.concurrent.Executor exec])
  (doRun [^clojure.lang.IFn f ^clojure.lang.ISeq args])
  (setErrorMode [^clojure.lang.Keyword k])
  (^clojure.lang.Keyword getErrorMode [])
  (setErrorHandler [^clojure.lang.IFn f])
  (^clojure.lang.IFn getErrorHandler [])
  (restart [new-state clear-action?])
  (releasePendingSends [])
  (setState [newval])
  (execute [^Runnable action])
  (enqueue [^Runnable action]))
