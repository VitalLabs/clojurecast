(ns clojurecast.services.repl
  (:refer-clojure :exclude [sync])
  (:require [com.stuartsierra.component :as com]
            [clojure.tools.nrepl.server :as nrepl-server]
            [cider.nrepl :refer [cider-nrepl-handler]]
            [clojurecast.services :as svc]
            [clojurecast.action-lists :as actions :refer [define-action-list]]
            [clojure.edn :as edn]))

(defrecord Repl [server]
  svc/Service
  (start-service [service]
    (let [{:keys [port handler]} (svc/configuration service)]
      (assoc service
             :server (nrepl-server/start-server :port (int port)
                                                :handler handler)
             :state :service.state/started)))
  (stop-service [service]
    (nrepl-server/stop-server server)
    (assoc service
           :server nil
           :state :service.state/stopped))
  (service-state [service]
    (:state service))
  (service-defaults [service]
    {:port 4005
     :handler cider-nrepl-handler})
  (service-configuration [service]
    (let [m {:port (System/getProperty "cc.services.repl.port")
             :handler (System/getProperty "cc.services.repl.handler")}]
      (cond-> m
        (:handler m) (update :handler (comp resolve edn/read-string)))))
  (service-description [service]
    (select-keys service [:server])))
