# Clojurecast

Clojurecast is a clojure abstraction around the Hazelcast distributed
library.  It provides clojure protocols for Hazelcasts's distributed data
structures as well as some other higher order abstractions that are helpful
for rapidly building and optimizing clustered services.

Key features:

- Clustering w/ Lifecycle management
- Distributed components (ala Stuart Sierra's Components)
- Distributed data structures: Atom, Agent, Maps, MultiMaps, Queue, Set, List, Caching, etc.
- Distributed locks and countdown latches
- Pub/Sub communication via Manifold's stream abstractions
- Distributed workflow engine: in-memory event driven "state machines"
- Ring middleware for session support
- Uses Nippy for communication and storage serialization

# License

MIT License

# Clustering



# Distributed Data Structures

## Atom

## Map

## Agent

## 

# Workflow Subsystem

The Workflow subsystem subsumes much of what was previously supported
by the scheduler system.  A Workflow is behaves similarly to an Actor
in that it accepts an ordered sequence of messages from one or more
sources across the system, updates local state in response along
with any side effects to the database, system, or network.

Workflows are distributed, but each workflow runs on one owning node.
When they start up, they subscribe to one or more reliable topics and
maintain appropriate sequence IDs in the state so they can restart
no a new node after a migration or failover without losing messages.

Currently workflows are not intended to be used in a high throughput
scenario (e.g. a workflow is handling many thousands of requests per
second) but in a scenario where many workflows are getting one or more
messages.  A given workflow will serialize processing, but multiple
workflows can run in parallel.

## TODO - Describe workflow API and state here

# Clojurecast Scheduler

**Deprecated 9/30, will be removed in 1.0**

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

## Scheduler Subsystem Terminology (deprecated)

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

