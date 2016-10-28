(ns clojurecast.serialization
  (:require [clojure.edn :as edn]
            [taoensso.nippy :as nippy])
  (:import [com.hazelcast.nio.serialization StreamSerializer ByteArraySerializer]
           #_[com.hazelcast.config SerializerConfig ]))


;;
;; Register Serializers (Programmatic API)
;;

#_(defn configure-serializers
  "Given an HZ config object, add support for custom"
  [config]
  (let [sc (SerializersConfig.)]
    )
  (.addSerializerConfig config sc)
  )

(def nippy-serializer 42)

(deftype NippySerializer []
  ByteArraySerializer
  (getTypeId [this] nippy-serializer)
  (write [this object]
    (nippy/freeze object))
  (read [this bytes]
    (nippy/thaw bytes)))

(def creds [:salted "incant3r"])
(defn set-credentials!
  [password]
  (alter-var-root #'creds (fn [old] password)))

(deftype EncryptedNippySerializer []
  ByteArraySerializer
  (getTypeId [this] nippy-serializer)
  (write [this object]
    (nippy/freeze object {:password creds}))
  (read [this bytes]
    (nippy/thaw bytes {:password creds})))

(def edn-serializer 43)

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
