(ns clojurecast.lang.cache
  (:require [clojure.core.cache :as cache]
            [taoensso.nippy :as nippy]
            [clojurecast.lang.interfaces]))

(deftype Cache [])
