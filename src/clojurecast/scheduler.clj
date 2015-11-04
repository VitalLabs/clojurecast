(ns clojurecast.scheduler
  (:require [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [com.stuartsierra.component :as com])
  (:import [java.util.concurrent Executors ScheduledExecutorService]
           [java.util.concurrent ScheduledThreadPoolExecutor]
           [com.hazelcast.core Cluster MembershipListener EntryListener]
           [com.hazelcast.core MessageListener]
           [java.util.concurrent TimeUnit]))

(def ^:dynamic *jobs-name* "scheduler/jobs")

(def ^:dynamic *test-mode* false)

(def ^:dynamic *job*)

(declare reschedule)

(defmulti run (comp (juxt :job/type :job/state) #(.get %)))

(defmulti handle-message (fn [job-ref message]
                           [(:job/type (.get job-ref))
                            (:event/type message)]))

(defmethod handle-message :default
  [job-ref message]
  (assoc (.get job-ref)
    :job/state :job.state/running
    :job/timeout 0))

(defmethod handle-message [:job/t :job/touch]
  [job-ref message]
  (assoc (.get job-ref)
    :job/state :job.state/running
    :job/timeout 0))

(defn- job-message-listener
  [job-id]
  (reify MessageListener
    (onMessage [_ message]
      (let [job-ref (cc/atomic-reference job-id)
            job (handle-message job-ref (.getMessageObject message))]
        (.set job-ref job)
        (reschedule job)))))

(defn schedule
  [job]
  (.set (cc/atomic-reference (:job/id job))
        (assoc job
          :job/timeout (:job/timeout job 0)
          :job/state (:job/state job :job.state/running)
          :job/topic-bus (or (:job/topic-bus job)
                             (.addMessageListener
                              (cc/reliable-topic (:job/id job))
                              (job-message-listener (:job/id job))))))
  (.put (cc/multi-map *jobs-name*)
        (cluster/local-member-uuid)
        (:job/id job)))

(defn unschedule
  [job]
  (let [job-ref (cc/atomic-reference (:job/id job))]
    (when (:job/topic-bus (.get job-ref))
      (.removeMessageListener (cc/reliable-topic (:job/id job))
                              (:job/topic-bus (.get job-ref)))))
  (.remove (cc/multi-map *jobs-name*)
           (cluster/local-member-uuid)
           (:job/id job)))

(defmethod run [:job/t :job.state/pausing]
  [job-ref]
  (let [job (.get job-ref)]
    (.remove (cc/multi-map *jobs-name*)
             (cluster/local-member-uuid)
             (:job/id job))
    (assoc job
      :job/state :job.state/paused
      :job/timeout 0)))

(defmethod run :default
  [job-ref]
  (assoc (.get job-ref)
    :job/state :job.state/pausing
    :job/timeout 0))

(defn- scheduler-membership-listener
  []
  (let [jobs (cc/multi-map *jobs-name*)]
    (reify MembershipListener
      (memberAdded [_ e])
      (memberAttributeChanged [_ e])
      (memberRemoved [_ e]
        (when (cluster/is-master?)
          (let [removed-member (.getMember e)
                outstanding (.get jobs (.getUuid removed-member))]
            (when (seq outstanding)              
              (loop [members (map #(.getUuid %) (cluster/members))
                     parts (partition (count (cluster/members)) outstanding)]
                (when (seq members)
                  (let [member (first members)
                        job-uuids (first parts)]
                    (doseq [uuid job-uuids]
                      (.put jobs member uuid))
                    (recur (next members) (next jobs))))))))))))

(defn- ^Callable job-callable
  [job-ref]
  (fn []
    (try
      (binding [*job* job-ref]
        (run job-ref))
      (catch Throwable e
        (let [job (.get job-ref)]
          (.remove (cc/multi-map *jobs-name*)
                   (cluster/local-member-uuid)
                   (:job/id job))
          (assoc job
            :job/state :job.state/failed
            :job/error e
            :job/timeout 0))))))

(defn- run-job
  [job-id exec tasks]
  (let [job-ref (cc/atomic-reference job-id)
        scheduled-future (.schedule exec
                                    (job-callable job-ref)
                                    (if *test-mode*
                                      0
                                      (:job/timeout (.get job-ref)))
                                    TimeUnit/MILLISECONDS)]
    (swap! tasks assoc job-id scheduled-future)
    (future
      (when-let [job @scheduled-future]
        (swap! tasks dissoc job-id)
        (.set job-ref job)
        (cond
          (.isCancelled scheduled-future) :cancel
          (= (:job/state job) :job.state/paused) :pause
          (= (:job/state job) :job.state/failed) :failed
          :else (run-job job-id exec tasks))))))

(defn- job-entry-listener
  [exec tasks]
  (reify EntryListener
    (entryAdded [_ e]
      (run-job (.getValue e) exec tasks))
    (entryRemoved [_ e]
      (let [job-id (.getOldValue e)]
        (when-let [task (get tasks job-id)]
          (.cancel task false)
          (swap! tasks dissoc job-id))))))

(defrecord Scheduler [^com.hazelcast.core.MultiMap jobs
                      ^ScheduledExecutorService exec
                      ^String membership-listener-id
                      ^String entry-listener-id
                      tasks]
  com/Lifecycle
  (start [this]
    (if exec
      this
      (let [jobs (cc/multi-map *jobs-name*)
            exec (doto (Executors/newScheduledThreadPool 1)
                   (.setRemoveOnCancelPolicy true))
            tasks (atom {})
            listener (scheduler-membership-listener)
            job-listener (job-entry-listener exec tasks)]
        (assoc this
          :jobs jobs
          :exec exec
          :membership-listener-id (cluster/add-membership-listener listener)
          :entry-listener-id (.addEntryListener jobs job-listener
                                                (cluster/local-member-uuid)
                                                true)
          :tasks tasks))))
  (stop [this]
    (if exec
      (do
        (.shutdown exec)
        (.removeEntryListener jobs entry-listener-id)
        (cluster/remove-membership-listener membership-listener-id)
        (assoc this :jobs nil :exec nil))
      this)))

(defn reschedule
  [job]
  (let []
    (.remove (cc/multi-map *jobs-name*)
             (cluster/local-member-uuid)
             (:job/id job))
    (.put (cc/multi-map *jobs-name*)
          (cluster/local-member-uuid)
          (:job/id job))))

