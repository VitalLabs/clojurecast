(ns clojurecast.core
  (:require [com.stuartsierra.component :as com]))

(defrecord Node [instance]
  com/Lifecycle
  (start [this]
    (if instance
      this
      this))
  (stop [this]
    (if instance
      this
      this)))



