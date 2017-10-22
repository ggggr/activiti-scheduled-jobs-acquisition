package org.ggrittt.activiti.engine.impl.asyncexecutor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.activiti.engine.ActivitiOptimisticLockingException;
import org.activiti.engine.impl.asyncexecutor.AcquiredJobEntities;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.cmd.AcquireJobsCmd;
import org.activiti.engine.impl.interceptor.CommandExecutor;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 

 */
public class AcquireAsyncJobsDueRunnable {

	private final static Logger LOGGER = LoggerFactory.getLogger(AcquireAsyncJobsDueRunnable.class);

	private final AsyncExecutor asyncExecutor;

	private ScheduledExecutorService scheduledExecutorService;

	private volatile boolean isInterrupted;

	public AcquireAsyncJobsDueRunnable(ScheduledExecutorService scheduledExecutorService, AsyncExecutor asyncExecutor) {
		this.scheduledExecutorService = scheduledExecutorService ;
		this.asyncExecutor = asyncExecutor;
	}

	public void start() {
		scheduleTask(0);
	}
	
	public void stop() {
		isInterrupted = true;
	}

	private void scheduleTask(int delay) {
		LOGGER.info("Next execution of {} in {} ms.", getClass().getSimpleName(), delay);
		scheduledExecutorService.schedule(new Runnable() {
			public void run() {
				((DefaultAsyncJobExecutor)asyncExecutor).getExecutorService().submit(new AcquireAsyncJobsDueTask()) ;
			}
		}, delay, TimeUnit.MILLISECONDS);
	}

	private class AcquireAsyncJobsDueTask implements Runnable {
		public synchronized void run() {
			
			int millisToWait = asyncExecutor.getDefaultAsyncJobAcquireWaitTimeInMillis();
			boolean allJobsSuccessfullyOffered = true;
			try {
				
				final CommandExecutor commandExecutor = asyncExecutor.getProcessEngineConfiguration().getCommandExecutor();
				AcquiredJobEntities acquiredJobs = commandExecutor.execute(new AcquireJobsCmd(asyncExecutor));

				for (JobEntity job : acquiredJobs.getJobs()) {
					boolean jobSuccessFullyOffered = asyncExecutor.executeAsyncJob(job);
					if (!jobSuccessFullyOffered) {
						allJobsSuccessfullyOffered = false;
					}
				}
			
				int jobsAcquired = acquiredJobs.size();
				if (jobsAcquired >= asyncExecutor.getMaxAsyncJobsDuePerAcquisition()) {
					millisToWait = 0; 
				}

				// If the queue was full, we wait too (even if we got enough jobs back), as not overload the queue
				if (millisToWait == 0 && !allJobsSuccessfullyOffered) {
					millisToWait = asyncExecutor.getDefaultQueueSizeFullWaitTimeInMillis();
				}

			} catch (ActivitiOptimisticLockingException optimisticLockingException) {
				LOGGER.debug("Optimistic locking exception during async job acquisition. If you have multiple async executors running against the same database, "
				              + "this exception means that this thread tried to acquire a due async job, which already was acquired by another async executor acquisition thread."
				              + "This is expected behavior in a clustered environment. "
				              + "You can ignore this message if you indeed have multiple async executor acquisition threads running against the same database. " + "Exception message: {}",
				              optimisticLockingException.getMessage());
			} catch (Throwable e) {
				LOGGER.error("exception during async job acquisition: {}", e.getMessage(), e);
				millisToWait = asyncExecutor.getDefaultAsyncJobAcquireWaitTimeInMillis();
			}

			if (!isInterrupted) {
				scheduleTask(millisToWait);
			} else {
				LOGGER.info("Stop requested.");
			}
		}
	}

}
