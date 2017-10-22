package org.ggrittt.activiti.engine.impl.asyncexecutor;

/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.activiti.engine.ActivitiOptimisticLockingException;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.asyncexecutor.FindExpiredJobsCmd;
import org.activiti.engine.impl.asyncexecutor.ResetExpiredJobsCmd;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.activiti.engine.runtime.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable that checks the {@link Job} entities periodically for 'expired' jobs.
 * 
 * When a job is executed, it is first locked (lock owner and lock time is set).
 * A job is expired when this lock time is exceeded. This can happen when an executor 
 * goes down before completing a task.
 * 
 * This runnable will find such jobs and reset them, so they can be picked up again.
 * 

 */
public class ResetExpiredJobsRunnable {

	private static Logger LOGGER = LoggerFactory.getLogger(ResetExpiredJobsRunnable.class);

	protected final AsyncExecutor asyncExecutor;
	private ScheduledExecutorService scheduledExecutorService ;

	protected volatile boolean isInterrupted ;

	public ResetExpiredJobsRunnable(ScheduledExecutorService scheduledExecutorService, AsyncExecutor asyncExecutor) {
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
				((DefaultAsyncJobExecutor)asyncExecutor).getExecutorService().submit(new ResetExpiredJobsRunnableTask()) ;
			}
		}, delay, TimeUnit.MILLISECONDS);
	}

	private class ResetExpiredJobsRunnableTask implements Runnable {
		public synchronized void run() {
			LOGGER.info("Reset expired jobs");

			try {

				List<JobEntity> expiredJobs = asyncExecutor.getProcessEngineConfiguration().getCommandExecutor()
						.execute(new FindExpiredJobsCmd(asyncExecutor.getResetExpiredJobsPageSize()));

				List<String> expiredJobIds = new ArrayList<String>(expiredJobs.size());
				for (JobEntity expiredJob : expiredJobs) {
					expiredJobIds.add(expiredJob.getId());
				}

				if (expiredJobIds.size() > 0) {
					asyncExecutor.getProcessEngineConfiguration().getCommandExecutor()
					.execute(new ResetExpiredJobsCmd(expiredJobIds));
				}

			} catch (Throwable e) {
				if (e instanceof ActivitiOptimisticLockingException) {
					LOGGER.debug("Optmistic lock exception while resetting locked jobs", e);
				} else {
					LOGGER.error("exception during resetting expired jobs", e.getMessage(), e);
				}
			}
			
			if (!isInterrupted) {
				scheduleTask(asyncExecutor.getResetExpiredJobsInterval());
			} else {
				LOGGER.info("Stop requested.");
			}

		}
	}

}
