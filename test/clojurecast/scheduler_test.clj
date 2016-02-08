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

(def ^:dynamic *node* nil)

(defn with-mock-jobs
  "Establishes jobs for each mock system."
  [f]
  (binding [*jobs* []]
    (let [{:keys [node1 scheduler1 node2 scheduler2]} system]

      ;; invoke tests with scheduler1
      (with-scheduler scheduler1
        (println :node1 (cluster/local-member-uuid))
        (doseq [job (gen/sample job 10)]
          (set! *jobs* (conj *jobs* (:job/id job)))
          (schedule job))
        (binding [*node* node1]
          (f)))

      ;; invoke tests with scheduler2
      (with-scheduler scheduler2
        (println :node2 (cluster/local-member-uuid))
        (doseq [job (gen/sample job 10)]
          (set! *jobs* (conj *jobs* (:job/id job)))
          (schedule job))
        (binding [*node* node2]
          (f)))

      ;; unschedule everything
      (with-scheduler scheduler1
        (doseq [[k v] (cluster-jobs)]
          (unschedule k)))

      ;; assert every job has been created, updated, and removed
      (doseq [job-id *jobs*
              :let [history (get-job-history job-id)]]
        (is (= (first (first history)) :create)
            "First event recorded for job was not :create.")
        (is (= (first (peek history)) :remove)
            "Last event recorded for job was not :remove.")))))

(use-fixtures :once with-mock-system with-mock-jobs)

(defn- ensure-node-binding
  "Ensure when *node* is bound, it is bound with the identical hazelcast
  instance the code is being run against."
  [f]
  (when *node*
    (is (= (cluster/local-member-uuid (:instance *node*))
           (cluster/local-member-uuid))))
  (f))

(use-fixtures :each ensure-node-binding)

(deftest scheduler-is-bound
  (is *scheduler* "Scheduler is unbound."))

(deftest every-job-is-available
  (is *jobs* "Jobs are unbound.")
  (when *jobs*
    (doseq [job-id *jobs*]
      (is (get-job job-id) "Job is not found."))))


