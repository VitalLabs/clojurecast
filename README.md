# Clojurecast Scheduler

The cluster maintains a distributed map of jobs (job-id ->
job-state). Job state should be a clojure map that contains only
serializable values.

By contract, the cluster will maintain a single core.async go-loop
that handles calling the job multi-method 'run' on startup and
timeout events and message arrival events.  When the cluster
migrates data to/from a node due to a node outage or controlled
lifecycle (e.g. rolling upgrade), the async loop and topic handler
is migrated to a successor node.

NOTE: Due to the use of topics, messages to jobs that arrive
during cluster migration will be lost.

## Implementation

Each cluster member is assigned a subset of values in the map by
the HazelCast partitioner.  When values are added to the map, they
are 'scheduled' which includes being setup to listen to job topic
events and are immediately passed to a core.async go process, the
"run loop".  When they are removed from the map, they are unscheduled
and their associated state and run loop is cleaned up.

The run loop will call the scheduler/run method with an initial
state and registered a control handler for updating the scheduler
run loop.

Jobs that receive events via the job topic bus have a message
handler after which a :resume input is sent to the control loop.

## Scheduler Subsystem Terminology

- Scheduler
  - Contains local entry listener ID and the set of local Controllers
- Distributed Job Map (each member owns subset of entries according to partitioning strategy)
- Run Loop - a core.async go-loop that executes the workflow
- Listener - a topic listener for this job to receive messages
- Controller ('ctrl') - a control channel that is used to manage the run loop
- LocalEntryListener
  - Called when member when owned key-value pairs are added/removed
  - Registers a listener to the job's topic
  - Starts the local core.async loop
- MigrationListener
  - Called when partitions are being migrated
  - Used to shut down Run Loop and Listener (if source is still up)
  - Creates new Run Loop and Listener on new cluster


  
