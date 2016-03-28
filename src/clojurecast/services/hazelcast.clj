(ns clojurecast.services.hazelcast
  (:require [clojurecast.services :as svc]
            [clojurecast.action-lists :as actions :refer [define-action-list
                                                          define-action]]
            [clojure.edn :as edn])
  (:import [com.hazelcast.core HazelcastInstance]))

(define-action-list "Hazelcast start"
  :doc "Actions to run after the Hazelcast service has started."
  :sort-time :define-action)

(define-action-list "Hazelcast stop"
  :doc "Actions to run after the Hazelcast service has stopped."
  :sort-time :define-action)

(defprotocol HazelcastService
  (^HazelcastInstance get-instance [service])
  (^String get-password [service]))

(defrecord Hazelcast [^HazelcastInstance instance]
  svc/Service
  (start-service [service]
    (assoc service
           :instance (com.hazelcast.core.Hazelcast/newHazelcastInstance)
           :state :service.state/started))
  (stop-service [service]
    (.shutdown instance)
    (assoc service
           :instance nil
           :state :service.state/stopped))
  (service-state [service]
    (:state service))
  (service-defaults [service]
    {})
  (service-configuration [service]
    {:password (System/getProperty "cc.services.node.password")})
  (service-description [service]
    (select-keys service [:instance]))

  HazelcastService
  (get-instance [service]
    instance)
  (get-password [service]
    (:password (svc/configuration service))))

(define-action "Service start" "Hazelcast start"
  (fn [service]
    (when (instance? Hazelcast service)
      (actions/execute-actions "Hazelcast start" service))))

(define-action "Service stop" "Hazelcast stop"
  (fn [service]
    (when (instance? Hazelcast service)
      (actions/execute-actions "Hazelcast stop" service))))

