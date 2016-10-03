;; Copyright (c) Vital Labs, Inc. All rights reserved.  The use and
;; distribution terms for this software are covered by the MIT
;; License (https://opensource.org/licenses/MIT) which can be found
;; in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this notice,
;; or any other, from this software.

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
