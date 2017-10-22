package org.ggrittt.activiti.engine.impl.asyncexecutor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.activiti.engine.ActivitiOptimisticLockingException;
import org.activiti.engine.impl.asyncexecutor.AcquiredTimerJobEntities;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.asyncexecutor.JobManager;
import org.activiti.engine.impl.cmd.AcquireTimerJobsCmd;
import org.activiti.engine.impl.interceptor.Command;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.interceptor.CommandExecutor;
import org.activiti.engine.impl.persistence.entity.TimerJobEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 

 */
public class AcquireTimerJobsRunnable {

	private final static Logger LOGGER = LoggerFactory.getLogger(AcquireTimerJobsRunnable.class);

	private ScheduledExecutorService scheduledExecutorService;
	protected final AsyncExecutor asyncExecutor;
	protected final JobManager jobManager;

	protected volatile boolean isInterrupted;

	public AcquireTimerJobsRunnable(ScheduledExecutorService scheduledExecutorService, AsyncExecutor asyncExecutor, JobManager jobManager) {
		this.scheduledExecutorService = scheduledExecutorService;
		this.asyncExecutor = asyncExecutor;
		this.jobManager = jobManager;
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
				((DefaultAsyncJobExecutor)asyncExecutor).getExecutorService().submit(new AcquireTimerJobsRunnableTask()) ;
			}
		}, delay, TimeUnit.MILLISECONDS);
	}

	private class AcquireTimerJobsRunnableTask implements Runnable {
		public synchronized void run() {
			LOGGER.info("Acquire async jobs due");

			int millisToWait = asyncExecutor.getDefaultTimerJobAcquireWaitTimeInMillis();
			try {
				final CommandExecutor commandExecutor = asyncExecutor.getProcessEngineConfiguration().getCommandExecutor();


				final AcquiredTimerJobEntities acquiredJobs = commandExecutor.execute(new AcquireTimerJobsCmd(asyncExecutor));

				commandExecutor.execute(new Command<Void>() {

					public Void execute(CommandContext commandContext) {
						for (TimerJobEntity job : acquiredJobs.getJobs()) {
							jobManager.moveTimerJobToExecutableJob(job);
						}
						return null;
					}

				});

				int jobsAcquired = acquiredJobs.size();
				if (jobsAcquired >= asyncExecutor.getMaxTimerJobsPerAcquisition()) {
					millisToWait = 0;
				}
			} catch (ActivitiOptimisticLockingException optimisticLockingException) {
				LOGGER.debug("Optimistic locking exception during timer job acquisition. If you have multiple timer executors running against the same database, "
			              + "this exception means that this thread tried to acquire a timer job, which already was acquired by another timer executor acquisition thread."
			              + "This is expected behavior in a clustered environment. "
			              + "You can ignore this message if you indeed have multiple timer executor acquisition threads running against the same database. " + "Exception message: {}",
			              optimisticLockingException.getMessage());
			} catch (Throwable e) {
				LOGGER.error("exception during timer job acquisition: {}", e.getMessage(), e);
				millisToWait = asyncExecutor.getDefaultTimerJobAcquireWaitTimeInMillis();
			}

			if (!isInterrupted) {
				scheduleTask(millisToWait);
			} else {
				LOGGER.info("Stop requested.");
			}
		}

	}

}
