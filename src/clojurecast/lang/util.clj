;; Copyright (c) Vital Labs, Inc. All rights reserved.  The use and
;; distribution terms for this software are covered by the MIT
;; License (https://opensource.org/licenses/MIT) which can be found
;; in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this notice,
;; or any other, from this software.

(ns clojurecast.lang.util
  (:require [taoensso.nippy :as nippy]))

(defn- nippy-opts
  []
  (cond-> {}
    (System/getProperty "clojurecast.key")
    (assoc :password (System/getProperty "clojurecast.key"))))

(defn freeze
  [x]
  (when x
    (nippy/freeze x (nippy-opts))))

(defn thaw
  [x]
  (when x
    (nippy/thaw x (nippy-opts))))
