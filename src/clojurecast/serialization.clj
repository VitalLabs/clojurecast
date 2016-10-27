(ns clojurecast.serialization
  (:require [clojure.edn :as edn]
            [taoensso.nippy :as nippy])
  (:import [com.hazelcast.nio.serialization StreamSerializer]))


(defconst nippy-serializer 42)

(deftype NippySerializer []
  StreamSerializer
  (getTypeId [this] nippy-serializer)
  (write [this out object]
    (let [bytes (nippy/freeze object)]
      (.writeByteArray out bytes)))
  (read [this in]
    (let [bytes (.readByteArray in)]
      (nippy/thaw bytes))))

(defconst edn-serializer 43)

;; NOTE: This is not efficient!
(deftype EDNSerializer []
  StreamSerializer
  (getTypeId [this] edn-serializer)
  (write [this out object]
    (let [string (pr-str object)]
      (.writeCharArray out (.toCharArray bytes))))
  (read [this in]
    (let [chars (.readCharArray in)]
      (edn/read-string (String. chars)))))
