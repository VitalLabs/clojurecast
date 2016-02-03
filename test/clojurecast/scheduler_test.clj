(ns clojurecast.scheduler-test
  (:use clojure.test
        clojurecast.fixtures
        clojurecast.scheduler
        clojurecast.generators)
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [clojurecast.component :as com]
            [clojure.core.async :as async]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))

(derive :job/test :job/t)

(defmethod run [:job/test :job.state/running]
  [job]
  (assoc job
         :job/state :job.state/paused
         :job/timeout 0))

(def ^:dynamic *jobs* nil)

(defn with-mock-jobs
  "Establishes jobs for each mock system."
  [call-next-method]
  (binding [*jobs* []]
    (let [{:keys [node1 scheduler1 node2 scheduler2]} system]
      (with-system1
        (doseq [job (gen/sample job)]
          (set! *jobs* (conj *jobs* (:job/id job)))
          (schedule job)))
      (with-system2
        (doseq [job (gen/sample job)]
          (set! *jobs* (conj *jobs* (:job/id job)))
          (schedule job)))
      (call-next-method)
      ;; unschedule everything
      (with-system1
        (doseq [[k v] (cluster-jobs)]
          (unschedule k)))
      (with-system2
        (doseq [[k v] (cluster-jobs)]
          (unschedule k))))))

(use-fixtures :once with-mock-system with-mock-jobs)

(deftest scheduler-is-bound
  (is *scheduler* "Scheduler is unbound."))

(deftest every-job-is-available
  (is *jobs* "Jobs are unbound.")
  (when *jobs*
    (doseq [job-id *jobs*]
      (is (get-job job-id) "Job is not found."))))


