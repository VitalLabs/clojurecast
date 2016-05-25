(ns clojurecast.generators
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))

(def job
  "Generates a random job."
  (gen/hash-map :job/id gen/uuid
                :job/type (gen/return :job/test)))
