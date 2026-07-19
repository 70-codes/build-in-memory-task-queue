# Thinking Log

<!-- This is your scratchpad. Fill it in AS YOU GO, not at the end.
     Rough, fragmentary, honest. Don't polish it.
     Read the README for guidance on how to use this file. -->

## Initial Reaction

<!-- First 5 minutes, before you touch any code.
     - What's your gut take on the problem?
     - What feels like the hard part?
     - What approaches do you see? Which would you rule out and why?
     - Anything you're already unsure about? -->
in-memory queue with concurrency, delays, retry/backoff, DLQ and shutdown
no redis, no RabittMQ, no clickhouse using standard library and threads only
Delayed/retry tasks shouldn't hold worker slots while waiting sleeping inside a worker would be easier but wastes concurrency
Shutdown semantics need cares since if something is waiting for a 10 minute retry when shutdown happens I probably don't want shutdown waiting 10 minutes
hard sections 
- scheduling delayed tasks and retry backoff without overworking the cpu
- handling multiple threads safely which are engueuing while workers are dequeing

## Plan

<!-- Still before coding (or right at the start).
     - How will you structure this? Files, types, main components.
     - What are the key design decisions you're making up front?
     - What are you deliberately deferring?
     - What will you build FIRST — the smallest slice that proves something useful? -->

- create the project skeleton first
- define task/state types
- basic worker pool + immediate enqueue
- delayed scheduling
- retry/backoff
- DLQ
- shutdown
- concurrent enqueue test
- demo runner last

Probably BinaryHeap ordered by ready_at so workers can sleep until next task is ready.
Need Condvar so enqueue can wake sleeping workers when a new earlier task arrives.

## Progress Notes

<!-- Drop an entry any time you:
     - change direction from your plan
     - hit something unexpected
     - make a trade-off
     - realise you were wrong about something
     - finish a chunk and start the next

     One or two sentences each is fine. Timestamp each one.
     Imagine your pair partner just asked "what are you doing?" — answer that.
     Add as many entries as you need. -->

### [20:34]
Read the task and settled on worker threads + one shared scheduled queue.
Created the project skeleton. No queue logic yet, just getting the structure in place before I start with task/state types.

### [22:15]
Set up the main queue types and shared state using one Mutex around queue state and a Condvar for worker wakeups. enqueue assigns ids and pushes tasks into the shared heap, while shutdown flips a flag and waits for worker threads to finish.

### [22:34]
Implemented the worker loop where workers wait on the Condvar when there is no work and delayed tasks stay in the heap until ready_at instead of holding a worker slot failed tasks are pushed back into the heap with exponential backoff, and once retries are exhausted they go into the dead letter queue

I also made the shutdown behavior explicit where workers finish whatever is already running, but queued/delayed/retry-waiting tasks are not started once shutdown begins since I did not want shutdown blocked by something sitting in backoff for several minutes
### [HH:MM]

### [HH:MM]

## Research / References

<!-- Optional. Any docs, articles, past code, or language references you looked at.
     A one-line note on what you took from each is enough. -->
https://doc.rust-lang.org/std/sync/struct.Condvar.html - I used it for ordering tasks by ready_at and had to reverse Ord since BinaryHeap is a max-heap
https://doc.rust-lang.org/std/sync/struct.Condvar.html - I used it for worker sleep/wakeup so workers don't poll the queue

## Retrospective

<!-- After you're done. This section is NOT optional — it's one of the most
     valuable parts of the submission. Be honest.

     - What's the weakest part of your solution? Where's the duct tape?
     - Where would this break in production?
     - What would you do differently with more time?
     - What surprised you about this problem?
     - Anything you tried and threw away? Why? -->
