;; Copyright (c) Vital Labs, Inc. All rights reserved.  The use and
;; distribution terms for this software are covered by the MIT
;; License (https://opensource.org/licenses/MIT) which can be found
;; in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be
;; bound by the terms of this license.  You must not remove this notice,
;; or any other, from this software.

(ns clojurecast.lang.cache
  (:require [clojure.core.cache :as cache]
            [clojurecast.lang.util :as util]
            [clojurecast.lang.interfaces]))

(deftype Cache [])
