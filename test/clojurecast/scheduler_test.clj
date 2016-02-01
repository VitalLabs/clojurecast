(ns clojurecast.scheduler-test
  (:use clojure.test
        clojurecast.fixtures)
  (:require [clojurecast.core :as cc]
            [clojurecast.scheduler :as s]
            [clojurecast.cluster :as cluster]
            [clojurecast.component :as com]
            [clojure.core.async :as async]))

(use-fixtures :once with-mock-system)

(deftest ensure-node-listeners  
  (let [{:keys [node1 scheduler1 node2 scheduler2]} system]
    ))
