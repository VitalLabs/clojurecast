(ns clojurecast.services.datomic
  (:require [datomic.api :as d]
            [clojurecast.services :as svc]
            [clojurecast.action-lists :as actions :refer [define-action-list
                                                          define-action]]
            [clojure.edn :as edn]))

(defprotocol DatomicService
  (get-connection [service]
    "Returns the connection for the Datomic database."))

(define-action-list "Datomic start"
  :doc "Actions to run after the Datomic service has started."
  :sort-time :define-action)

(define-action-list "Datomic stop"
  :doc "Actions to run after the Datomic service has stopped."
  :sort-time :define-action)

(defrecord Datomic [conn]
  svc/Service
  (start-service [service]
    (let [{:keys [uri]} (svc/configuration service)]
      (d/create-database uri)
      (assoc service
             :conn (d/connect uri)
             :state :service.state/started)))
  (stop-service [service]
    (let [{:keys [uri destroy?]} (svc/configuration service)]
      (when destroy?
        (d/delete-database uri))
      (assoc service
             :conn nil
             :state :service.state/stopped)))
  (service-state [service]
    (:state service))
  (service-defaults [service]
    {:uri "datomic:mem://dev"
     :destroy? true})
  (service-configuration [service]
    (let [m {:uri (System/getProperty "cc.services.datomic.uri")
             :destroy? (System/getProperty "cc.services.datomic.destroy?")}]
      (cond-> m
        (:destroy? m) (update :destroy? edn/read-string))))
  (service-description [service]
    (select-keys service [:conn]))

  DatomicService
  (get-connection [service]
    conn))

(define-action "Service start" "Datomic start"
  (fn [service]
    (when (instance? Datomic service)
      (actions/execute-actions "Datomic start" service))))

(define-action "Service stop" "Datomic stop"
  (fn [service]
    (when (instance? Datomic service)
      (actions/execute-actions "Datomic stop" service))))

(comment
  ;; Example "Datomic start" actions
  
  (define-action "Datomic start" "Install schema"
    (fn [service]
      (println "WARNING: Should install database schema here.")))

  (define-action "Datomic start" "Run migrations"
    (fn [service]
      (println "WARNING: Should run applicable database migrations here."))
    :after ["Install schema"]))
