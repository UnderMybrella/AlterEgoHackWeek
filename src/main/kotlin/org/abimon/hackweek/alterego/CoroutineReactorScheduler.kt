package org.abimon.hackweek.alterego

import kotlinx.coroutines.*
import reactor.core.Disposable
import reactor.core.scheduler.Scheduler
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

class CoroutineReactorScheduler(
    val scope: CoroutineScope = GlobalScope,
    val context: CoroutineContext = Dispatchers.Default
) : Scheduler, Scheduler.Worker {
    data class ReactorJob(val job: Job) : Disposable {
        /**
         * Cancel or dispose the underlying task or resource.
         *
         *
         * Implementations are required to make this method idempotent.
         */
        override fun dispose() = job.cancel()

        /**
         * Optionally return {@literal true} when the resource or task is disposed.
         * <p>
         * Implementations are not required to track disposition and as such may never
         * return {@literal true} even when disposed. However, they MUST only return true
         * when there's a guarantee the resource or task is disposed.
         *
         * @return {@literal true} when there's a guarantee the resource or task is disposed.
         */
        override fun isDisposed(): Boolean = job.isCancelled || job.isCompleted
    }

    /**
     * Schedules the non-delayed execution of the given task on this scheduler.
     *
     *
     *
     * This method is safe to be called from multiple threads but there are no
     * ordering guarantees between tasks.
     *
     * @param task the task to execute
     *
     * @return the [Disposable] instance that let's one cancel this particular task.
     * If the [Scheduler] has been shut down, throw a [RejectedExecutionException].
     */
    override fun schedule(task: Runnable): Disposable = ReactorJob(scope.launch(context) { task.run() })

    override fun schedule(task: java.lang.Runnable, delay: Long, unit: TimeUnit): Disposable =
        ReactorJob(scope.launch { delay(unit.toMillis(delay)) })

    override fun schedulePeriodically(
        task: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit
    ): Disposable = ReactorJob(scope.launch {
        delay(unit.toMillis(initialDelay))
        val periodMs = unit.toMillis(period)
        while (isActive) {
            task.run()
            delay(periodMs)
        }
    })

    /**
     * Creates a worker of this Scheduler.
     *
     *
     * Once the Worker is no longer in use, one should call dispose() on it to
     * release any resources the particular Scheduler may have used.
     *
     * It depends on the implementation, but Scheduler Workers should usually run tasks in
     * FIFO order. Some implementations may entirely delegate the scheduling to an
     * underlying structure (like an [ExecutorService]).
     *
     * @return the Worker instance.
     */
    override fun createWorker(): Scheduler.Worker = this
}