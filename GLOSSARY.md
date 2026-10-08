# SuperJobs

The domain language for defining, submitting, and observing durable background job executions.

## Language

**Job**:
A hierarchically named contract with an immutable version describing the request, result, and optional intermediate event types for an operation.
_Avoid_: Job execution, attempt

**Contract package**:
A shared Python package containing Job definitions and their request, result, and optional intermediate event types, usable by producers and workers independently of worker implementation code.
_Avoid_: Worker package, job handler

**Job execution**:
One accepted submission of a Job, with a stable identity across its lifetime and any retries.
_Avoid_: Job, attempt, task

**Attempt**:
One delivery and processing try belonging to a Job execution. A retry creates another attempt without creating another execution.
_Avoid_: Job execution, duplicate job

**Idempotency key**:
A caller-provided key, scoped to a caller and Job version, used with a submission fingerprint to recognize equivalent repeated submissions of the same intended Job execution.
_Avoid_: Job ID, message ID

**Job handle**:
A reusable client-side reference to one Job execution, used to obtain its status, events, result, or outcome; it can be reconstructed from the execution identity and observed independently by multiple clients.
_Avoid_: Job, attempt

**At-least-once execution**:
A delivery guarantee under which a Job execution may be processed more than once, so handlers and their external effects must tolerate duplicate attempts.
_Avoid_: Exactly-once execution

**SuperJobs runtime**:
The process-level context through which applications register local handlers, submit Jobs, and manage the library lifecycle.
_Avoid_: Job worker, Job client

**Job contract**:
The immutable definition of a Job identity and its request, result, and optional intermediate event types.
_Avoid_: Worker capabilities, worker registration

**Contract manifest**:
The immutable shared record of a Job contract used to establish agreement between independently deployed participants.
_Avoid_: Worker registry, worker presence

**Worker instance**:
A running SuperJobs runtime that offers one or more Job handlers under an identity shared by its handler registrations.
_Avoid_: Permanent worker identity, host identity

**Worker registration**:
The current snapshot describing one worker instance's handler for one Job, including its presence and optional application capabilities.
_Avoid_: Job contract, contract manifest

**Worker presence**:
A time-limited declaration that a worker instance offers an execution-ready handler for a Job.
_Avoid_: Guaranteed availability, routing reservation

**Worker capabilities**:
Application-defined metadata describing the abilities of a worker instance's handler for a particular Job.
_Avoid_: Job contract, resource allocation, live telemetry

**Worker capability type**:
The application-defined structure of capability snapshots associated with a Job, shared by producers and workers independently of a worker instance's current capability values.
_Avoid_: Worker capabilities, Job request type, Job contract

**Raw worker capabilities**:
An uninterpreted worker capability payload accompanied by its data-format information, available for inspection without the application's capability type.
_Avoid_: Typed worker capabilities, validated application capabilities

**Worker discovery**:
Reading worker registrations to find instances currently offering a Job and inspect their declared capabilities.
_Avoid_: Job routing, scheduling, contract agreement

**Offered Job**:
A Job identified by at least one worker registration whose ready presence is currently valid.
_Avoid_: Known contract, locally registered Job, guaranteed availability

**Worker concurrency**:
The number of different Job executions a SuperJobs runtime may process at the same time within the automatically load-balanced pool for a Job; a value of one requires the current Attempt to finish before another request is consumed.
_Avoid_: Consumer group, attempt concurrency

**Load-balanced Job pool**:
All SuperJobs runtimes handling the same Job participate in one automatically shared pool of executions, without caller-selectable worker groups.
_Avoid_: Consumer group, broadcast group

**Progress**:
The latest known state of work within the current Attempt of a Job execution, where older updates may be discarded without losing an occurrence.
_Avoid_: Intermediate event

**Intermediate event**:
An application-defined occurrence emitted during a Job execution whose individual delivery is preserved and ordered when the execution completes normally, but is not required to survive worker loss.
_Avoid_: Progress, log message

**Job log**:
A structured, ordered observation produced by a Job execution and made available to clients observing that execution after batch publication; it has the same loss-on-worker-loss semantics as an intermediate event.
_Avoid_: Process log, diagnostic-only log

**Cancellation request**:
A request to stop a Job execution cooperatively; it is distinct from the execution reaching the terminal cancelled state.
_Avoid_: Cancelled execution, interruption

**Job outcome**:
The terminal success, failure, or cancellation of a Job execution, including its result or terminal reason.
_Avoid_: Job event, attempt

**Final result**:
The authoritative typed output of a successfully completed Job execution, retained independently of intermediate observations and expected to survive worker loss.
_Avoid_: Intermediate event, progress

**Observation history**:
The retained, ordered sequence of system events, intermediate events, progress snapshots, and Job logs associated with a Job execution.
_Avoid_: Live subscription, event batch

**Observation cursor**:
A sequence position used to resume reading an execution's observation history without silently skipping retained observations.
_Avoid_: Job ID, attempt number

**Terminal event**:
The final observation that records a Job execution's completed, failed, or cancelled state and closes its observation stream.
_Avoid_: Final result, intermediate event

**Job handler**:
Application code that processes an Attempt of a Job execution and produces its final result and observations.
_Avoid_: Job, Job execution

**Retry policy**:
Rules that determine whether a failed Attempt creates another Attempt, including the total attempt limit and delay between attempts; cancellation and validation failures are not retried.
_Avoid_: Cancellation policy, timeout

**Job version**:
The immutable identity of a Job contract, supplied separately from its name; an absent version means unversioned and never means “latest.”
_Avoid_: Schema revision, latest version

**Job name**:
A lowercase dot-separated hierarchical identifier for a Job contract; its hierarchy is part of the name rather than a separate namespace/name model.
_Avoid_: Namespace, short name

**Job error**:
A stable, serializable description of why a Job execution or Attempt failed, independent of the exception class used inside a worker process.
_Avoid_: Remote exception, traceback

**Failure code**:
A stable classification of a Job error, such as a timeout or deadline expiration, that clients can handle without parsing human-readable messages.
_Avoid_: Exception name, log message

**Attempt timeout**:
A relative limit on how long an active Attempt may run before it fails with a timeout failure code.
_Avoid_: Execution deadline, due time

**Execution deadline**:
An absolute time limit for a Job execution, including time spent waiting and retrying.
_Avoid_: Attempt timeout, due time

**Observation retention**:
The age and size limits governing how long a Job execution's observation history remains available for replay.
_Avoid_: Result retention, live subscription

**Rejected message**:
A transport message that cannot be decoded or validated as a Job request and therefore does not create a Job execution.
_Avoid_: Failed Job, failed Attempt

**Completion record**:
The authoritative durable record of a terminal Job outcome and its final result, used to reconcile redeliveries without executing a completed Job again.
_Avoid_: Terminal event, observation

**Submission fingerprint**:
The canonical identity of a Job version, request, and execution-affecting submission options used to detect conflicting reuse of an idempotency key.
_Avoid_: Job ID, payload checksum

**Result retention**:
The configured period or size policy for which a Job execution's final result and completion record remain available.
_Avoid_: Observation retention, execution deadline
