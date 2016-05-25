(ns clojurecast.core-test
  (:use clojure.test
        clojurecast.fixtures)
  (:require [clojurecast.core :as cc]
            [clojurecast.component :as com]
            [clojure.core.async :as async]))

(use-fixtures :once with-mock-system)
