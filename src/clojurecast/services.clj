(ns clojurecast.services
  (:refer-clojure :exclude [sync])
  (:require [com.stuartsierra.component :as com]
            [clojurecast.action-lists :as actions :refer [define-action-list]]))

(define-action-list "Service start"
  :doc "Actions to run when a service starts.")

(define-action-list "Service stop"
  :doc "Actions to run when a service stops.")

(defprotocol Service
  (start-service [service]
    "Starts the local service. Returns an updated service record.")
  (stop-service [service]
    "Stops the local service. Returns an updated service record.")
  (service-state [service]
    "Returns at least one of :service.state/started,
    :service.state/stopped, or :service.state/error. Can return other
    values derived from these (via clojure.core/derive) with domain
    specific semantics.")
  (service-defaults [service]
    "Returns a map with the default configuration for this service.")
  (service-configuration [service]
    "Returns the active configuration map of the local service.")
  (service-description [service]
    "Returns a hash map describing the state of the service."))

(defprotocol AgentService
  (service-send [service action]
    "Dispatches an action to the service.")
  (service-validator [service]
    "Returns the service validator-fn for the service.")
  (set-service-validator! [service validator-fn]
    "Sets a new validator-fn for the service.")
  (restart-service [service]
    "Restarts the local service. Returns an updated service record.")
  (service-error [service]
    "Returns an Exception when service-state is :service.state/error,
    otherwise nil.")
  (service-error-mode [service]
    "Returns one of :service.error-mode/fail or
    :service.error-mode/continue. Can return other values derived from
    these (via clojure.core/derive) with domain specific semantics.")
  (set-service-error-mode! [service mode]
    "Sets one of :service.error-mode/fail or
    :service.error-mode/continue. Can set other values derived from
    these (via clojure.core/derive) with domain specific semantics.")
  (service-error-handler [service]
    "Returns the error handler for the service.")
  (set-service-error-handler! [service handler-fn]
    "Sets the error handler for the service."))

(defprotocol ResumableService
  (pause-service [service]
    "Pauses the local service. Returns an updated service record.")
  (resume-service [service]
    "Resumes the local service. Returns an updated service record."))

(defprotocol DistributedService
  (service-t [service]
    "Returns the local basis-t of the service.")
  (service-cluster-t [service]
    "Returns the distributed basis-t of the service.")
  (service-sync [service to-basis-t]
    "Synchronizes local service to new basis-t `to`. If `to` is
    ahead of service-t, the service should commit all commands between
    service-t and `to`. If `to` is behind service-t, the service
    should revert all commands between `to` and service-t.")
  (service-execute [service command-data]
    "Executes commands for service.")  
  (service-migrate [service from-basis-t to-basis-t]
    "Runs every applicable migration for service `from-basis-t` to
    `to-basis-t`. If no applicable migrations are registered, this
    operation should delegate to service-sync, where local
    transactions are instead distributed transactions.")
  (service-history [service] [service from-t] [service from-to to-t]
    "Returns a vector of all commands between from-t and to-t, inclusive."))

(defprotocol Command
  (commit [command]
    "Commits command.")
  (revert [command]
    "Reverts command."))

(defn service-started?
  "Returns true when service-state is :service.state/started, otherwise false."
  [service]
  (isa? (service-state service) :service.state/started))

(defn sync
  ([service]
   {:pre [(service-started? service)]}
   (service-sync service (service-cluster-t service)))
  ([service to-t]
   {:pre [(service-started? service)]}
   (service-sync service to-t)))

(defn execute
  [service command-data]
  {:pre [(service-started? service)]}
  (service-execute service command-data))

(defn migrate
  [service from-t to-t]
  {:pre [(service-started? service)]
   :post [(= (service-cluster-t service) to-t)]}
  (service-migrate service from-t to-t))

(defn history
  ([service]
   {:pre [(service-started? service)]}
   (service-history service))
  ([service from-t]
   {:pre [(service-started? service)]}
   (service-history service from-t))
  ([service from-t to-t]
   {:pre [(service-started? service)]}
   (service-history service from-t to-t)))

(defn pause
  [service]
  {:pre [(service-started? service)]}
  (pause-service service))

(defn resume
  [service]
  {:pre [(service-started? service)]}
  (resume-service service))

(defn configuration
  [service]
  (merge-with #(if (nil? %2) %1 %2)
              (service-defaults service)
              (service-configuration service)))

(defn start
  [service]
  (if (service-started? service)
    service
    (let [service (start-service service)]
      (actions/execute-actions "Service start" service)
      service)))

(defn stop
  [service]
  (if (service-started? service)
    (let [service (stop-service service)]
      (actions/execute-actions "Service stop" service)
      service)
    service))

(defn- print-service
  [service ^java.io.Writer writer]
  (.write writer "#")
  (.write writer (.getName (class service)))
  (#'clojure.core/print-map (service-description service)
                            print-method
                            writer))

(defn- print-record
  [record ^java.io.Writer writer]
  (#'clojure.core/print-meta record writer)
  (.write writer "#")
  (.write writer (.getName (class record)))
  (#'clojure.core/print-map record #'clojure.core/pr-on writer))

(defmethod print-method clojure.lang.IRecord
  [record ^java.io.Writer writer]
  (if (satisfies? Service record)
    (print-service record writer)
    (print-record record writer)))

(extend-protocol com/Lifecycle
  clojurecast.services.Service
  (start [this]
    (start this))
  (stop [this]
    (stop this)))


