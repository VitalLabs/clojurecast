(ns clojurecast.jobs
  (:require [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [clojurecast.core :as cc]
            [clojurecast.cluster :as cluster]
            [clojurecast.component :as com]
            [clojurecast.stream :as stream])
  (:import [java.util.concurrent DelayQueue Executors ScheduledExecutorService]
           [java.util.concurrent ScheduledFuture ScheduledThreadPoolExecutor]
           [com.hazelcast.core Cluster MembershipListener EntryListener]
           [com.hazelcast.core MessageListener MigrationListener]
           [com.hazelcast.map.listener MapListener]
           [com.hazelcast.map.listener
            EntryAddedListener EntryUpdatedListener EntryRemovedListener]
           [java.util.concurrent TimeUnit]))

;; Job Durability Semantics
;; - Topic messages are only deemed consumed when the appropriate
;;   sequence ID is saved to the cluster job map. Job run methods
;;   with side effects should be idempotent in case a transaction
;;   crashes before the update is committed.
;; - Local commands, such as timeouts, are only committed when
;;   the timeout command updates the job state and clears a specific
;;   piece of state (timeout-at in the timeout case).
;; - Race conditions.  If timeout-at doesn't match the current job
;;   state, the timeout command will be ignored.  This addresses
;;   the race condition where an event comes in before the timeout is
;;   cleared and a timeout command is queued after the event that would
;;   have cancelled it.  All other operational commands must check that
;;   the state is consistent if it's not part of the current thread's
;;   execution.

;; Job Lifecycle
;; - Created: Add valid job state to the cluster job map
;; - Added: Local instance that owns job attaches it
;; - Attach: Create local stream and add listeners to appropriate topics
;; - Migrate: Detach from old owner, attach on the new owner
;; - Detach: Remove listeners, release stream
;; - Delete: Remove job state from job map

;; Creating a new job
;; - Add the job to the cluster job map
;; - The owning node's JobEngine component
;;   - Listens for cluster job map add/remove/migrate
;;   - Listens to a global jobs topic for direct messages for jobs
;;   - Manages all timeouts for local jobs

;; Jobs:
;; - Single input sink that forces sequential updates to the job map
;;   in response to messages
;; - Transducer on source to filter/transform system events to messages?

;; System messages
;; - Directly talk to jobs via a topic
;; Datomic model messages
;; - Transaction metadata used to auto-generate events?

;; DistributedMap for job state
;; DistributedService
;; - Listens to migration events
;; 
;; {:job/type Keyword
;;  :job/id Str
;;  :job.io/topics {"topic" Int ...}
;;  :job.io/seqid {"topic" Int ...}
;;  :job.state/current Keyword -- last method executed
;;  :job.state/next Keyword -- 
;;  :job.state/timeout Keyword
;;  :job.state/timeout-at Inst
;;  :job.state/error Exception
;;  }
;; Manifold input stream for serial operation
;; One or more ReliableTopicListeners
;;

;; ================================
;; Switchboard Notes:
;; ================================

;; Big architectural changes:
;; - Live sessions listen to account and 1 subject at a time
;; - Jobs listen to a subject and/or account topic
;; - Jobs are basically reducers processing messages on a manifold
;;   stream that serializes transforms over job state.

;; Refactor sequencing part I
;; - Clojurecast refactor
;; - Clojurecast jobs (deprecate scheduler)
;; - Backpressure via HZ topics
;; - Async IO w/ jobs strategy
;; - Async Alia and Datomic via manifold
;; - Change submit-events to push into new topic model
;; - Forward port scheduler tasks into jobs

;; Refactor sequencing part II
;; - Durable side effects (e.g. Twilio IO) from jobs and API handlers
;; - Event bus and event object format
;; - Fix model creation globally
;; - Standardize datomic transaction and event generation
;; - Refactor handlers to be async

;; Subject + Jobs in-memory impact:
;; - Subject Topic - messages per epoch (max 1024) * average msg size
;; - Manifold stream x nW
;; - Job state x nW
;; - Topic listeners x nW x nT where nT ~ 1-3

;; Subject + live connection in-memory impact:
;; - Websocket handler
;; - Per-subject topic listener

;; Locality Strategy:
;; - Load balancer routing by subject ID or account ID (providers)
;; - Topic ring buffer partitioning by subject ID and account ID
;; - Job map partitioned by subject ID
;; - Session map partitioned by subject ID

;; Event bus v2.0
;; - Most model events injected by transaction listener
;;   (injected by owning node by subject ID or account ID?)
;; - Observation events injected by handler
;; - Derived client data needs rules for which events cause a recompute
;; - Events and Job State are defrecords for smarter dispatch?
;; - Sync handler has single subscription per job and list of attached websockets


(set! *warn-on-reflection* true)

(def ^:dynamic *job*)

(defonce ^:dynamic *engine* nil)

(defmacro with-engine
  [engine & body]
  `(let [engine# ~engine]
     (binding [cc/*instance* (get-in engine# [:node :instance])
               *engine* engine#]
       ~@body)))

(defn- ^com.hazelcast.core.HazelcastInstance engine-instance
  "Private getter for the HazelcastInstance which instantiated *engine*."
  ([]
   {:pre [*engine*]}
   (engine-instance *engine*))
  ([engine]
   (get-in engine [:node :instance])))

(defn ^com.hazelcast.core.IMap cluster-jobs
  "Public getter for the cluster-wide job map."
  ([]
   {:pre [*engine*]}
   (cluster-jobs (engine-instance)))
  ([instance]
   (cc/distributed-map instance "cluster-jobs")))

;;
;; JOB Implementors API
;;
;; These two methods are implemented by jobs to handle events and
;; transition between job states.
;;

(defmulti run
  "The job run method dispatches on :job/type and :job/state.
   :job/type namespaces the specific job class and :job/state 
   is a value that indicates the current state."
  (juxt :job/type :job.state/current))

(defmulti message-dispatch (fn [job message] (:job/type job)))

(defmethod message-dispatch :default [job message]
  (:event/type message))

(defmulti handle-message
  "The handle-message deals with out-of-band events.  It can
   modify state and/or start the job running in response to
   the event.  It is also namespaced by :job/type"
  (fn [job message]
    [(:job/type job)
     (message-dispatch job message)]))

;;
;; JOB History Hook
;;

(defn- job-history-fn
  "Returns the current history-fn set in config on the scheduler."
  []
  {:pre [*engine*]}
  (get-in *engine* [:config :history-fn]))

(defn- record-job-history
  "history-fn :: action -> job-state -> Nil"
  [action job-state]
  (when-let [cb (job-history-fn)]
    (when job-state
      (cb action job-state))))

(defn- ^MapListener job-entry-listener
  [engine ctrls]
  (reify
    EntryAddedListener
    (entryAdded [_ e]
      (with-engine engine
        (record-job-history :create (.getValue e))))
    EntryUpdatedListener
    (entryUpdated [_ e]
      (with-engine engine
        (record-job-history :update (.getValue e))))
    EntryRemovedListener
    (entryRemoved [_ e]
      (with-engine engine
        (record-job-history :remove (.getOldValue e))))))


;;
;; JOB Engine
;; 


;; Protocol for clients of the distributed jobs engine

(defprotocol IJobsEngine
  (start-job!  [job-state] "Add a job to the cluster's distributed workflow execution engine")
  (get-job     [job-id]    "Get the current job state for the job-id, or nil if job is not found")
  (send-to-job [job-id message] "Send a message to a specific job")
  (stop-job!   [job-id]         "Remove a job from the system"))

(defprotocol IJobsExecutor
  (add-timeout! [job-state])
  (save-state!  [job-state])
  (clear-state! [job-state]))

;;
;; JOB Local Initialization / Finalization
;;

(defn attach-job
  "Setup the job on the current node"
  [engine job-state]
  ;; Create and save job stream
  ;;    w/ transducer that invokes handle-message
  ;; Connect subscribed topic streams to job stream
  ;;    at last msg point + 1
  )

(defn detach-job
  "Disconnect the job listeners, etc. on the current node"
  [engine job-id]
  ;; Close main job stream, should close topic subscriptions too
  )


;;
;; Timeout Service
;;

(defprotocol ILocalScheduler
  (schedule-at [_ date f]))

;;
;; JOB Engine Stepper
;;

(defn- ensure-invariants
  "Verify that a state transform did not screw
   up special state."
  [old-state new-state]
  (and (= (:job/id old-state) (:job/id new-state))
       (:job.io/topics old-state)
       (:job.state/current new-state)
       (or (not (:job.state/timeout))
           (instance? java.util.Date (:job.state/timeout-at new-state)))))

(defn- step-state
  "Step the job from next state to the current state."
  [state]
  (-> state
      (assoc :job.state/current (:job.state/next state))
      (dissoc :job.state/next :job.state/timeout :job.state/timeout-at)))

(defn- command-timeout
  "Send a timeout command to the job associated with job-id"
  [engine job]
  (let [stream (job-stream engine job-id)
        date (:job.state/timeout-at state)
        next (:job.state/timeout state)]
    (assert (and stream next))
    (s/put! stream {:event/type :timeout :timeout/state next :timeout/date date})))

(defn- set-timeout
  [engine state]
  (if-let [target (:job.state/timeout-at state)]
    (schedule-at engine target #(command-timeout engine state))))

(defn step-job
  "Functional job update, given the current job state
   and designated job.state/next - execute the state 
   function which returns a copy of the state with
   a target next state or timeout state."
  [engine old-state]
  (->> (step-state old-state)
       (run)
       (ensure-invariants old-state)
       (set-timeout engine)
       (save-state! engine)))

;;
;; JOB Migration
;;
;; When jobs migrate to a new or backup instance, they need to
;; create a new local manifold stream and reconnect that stream
;; to any topics and re-establish any timeouts.  

(defn- ^MigrationListener migration-listener
  [engine]
  (reify
    MigrationListener
    (migrationStarted [_ e]
      (with-scheduler scheduler
        ;; TODO: Record when migration started?
        ))
    (migrationCompleted [_ e]
      (with-scheduler scheduler
        (let [partid (.getPartitionId e)]
          ;; I'm no longer the owner, detach async/loop and msg handler
          (when (.localMember (.getOldOwner e))
            (doseq [job-id (local-partition-keys (cluster-jobs) partid)]
              (detach-job scheduler job-id)))
          ;; I'm the new owner, attach async/loop & msg handler
          (when (.localMember (.getNewOwner e))
            (doseq [job-id (local-partition-keys (cluster-jobs) partid)]
              (attach-job scheduler job-id))))))
    (migrationFailed [_ e]
      (with-scheduler scheduler
        ;; TODO: How to handle failed migrations
        ))))

;;
;; 
;;

#_(defrecord JobsEngine [entry-id migration-id config node]
  com/Component
  (initialized? [_] true)
  (started? [_] (boolean entry_id))
  (migrate? [_] false)
  (-init [this] this)
  (-start [this]
    (if (thread-bound? #'*engine*)
      (set! *engine* this)
      (.bindRoot #'*engine* this))
    (let [jobs (cluster-jobs (:instance node))
          part (cc/partition-service (:instance node))
          this (assoc this
                      :config (update config :history-fn resolve-history-fn))
          eid  (.addLocalEntryListener jobs (job-entry-listener this))
          mid  (.addMigrationListener part (migration-listener this))
          local-jobs (seq (.localKeySet jobs))
          this (assoc this
                      :entry-id eid
                      :migration-id mid)]
      (if (thread-bound? #'*engine*)
        (set! *engine* this)
        (.bindRoot #'*engine* this))
      (doseq [job-id local-jobs]
        (attach-job this job-id))
      this))
  (-stop [this]
    (let [jobs (cluster-jobs (:instance node))]
      (doseq [job-id (seq (.localKeySet jobs))]
        (detach-job )))))
