import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.activiti.engine.ProcessEngine;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class StartEngine {

	public static void main(String[] args) throws IOException {

		int numberOfProcessInstances = 1000 ;

		ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("activiti-applicationContext.xml") ;
		final ProcessEngine processEngine = (ProcessEngine)applicationContext.getBean("processEngine") ;

		ExecutorService executorService = Executors.newFixedThreadPool(10,Executors.defaultThreadFactory());
		for (int i=0; i< numberOfProcessInstances ; i++) {
			executorService.submit(new Runnable() {
				public void run() {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					Map<String, Object> vars = new HashMap<String, Object>();
					processEngine.getRuntimeService().startProcessInstanceByKey("myProcess", vars);
				}
			});
		}

		PrintWriter printWriter = null;
		System.out.println("<Displaying stats enabled>");
		File outputFile = new File("C://Fujitsu//work//activiti//"+System.currentTimeMillis()+".txt");
		FileWriter fileWriter = new FileWriter(outputFile);
		printWriter = new PrintWriter(fileWriter);

		printWriter.println("timeStamp;nrOfProcessInstances;nrOfExecutions;nrOfTasks;nrOfAsyncJobs;nrFinishedProcessInstances;nrFinishedTasks;nrOfFinishedHistoricActInstances");

		boolean allDone = false;
		while (!allDone) {
			
			if( executorService.isTerminated()){
				executorService.shutdown();
			}

			long nrOfProcessInstances = processEngine.getRuntimeService().createProcessInstanceQuery().count();
			long nrOfExecutions = processEngine.getRuntimeService().createExecutionQuery().count();
			long nrOfTasks = processEngine.getTaskService().createTaskQuery().count();
			long nrOfAsyncJobs = processEngine.getManagementService().createJobQuery().count();
			long nrCompletedHistoricActivities = processEngine.getHistoryService().createHistoricActivityInstanceQuery().finished().count();

			long nrFinishedProcessInstances = processEngine.getHistoryService().createHistoricProcessInstanceQuery().finished().count();
			long nrFinishedTasks = processEngine.getHistoryService().createHistoricTaskInstanceQuery().finished().count();

			System.out.println();
			System.out.println("------------------------------------------------");
			Date timeStamp = new Date();
			System.out.println("Timestamp: " + new Date());
			System.out.println("Nr of process instances = " + nrOfProcessInstances);
			System.out.println("Nr of executions = " + nrOfExecutions);
			System.out.println("Nr of tasks = " + nrOfTasks);
			System.out.println("Nr of async / timer / DL jobs : " + nrOfAsyncJobs +".");
			System.out.println("Nr of finished process instances = " + nrFinishedProcessInstances);
			System.out.println("Nr of finished tasks = " + nrFinishedTasks);
			System.out.println("Nr of finished historic activities = " + nrCompletedHistoricActivities);
			System.out.println("Nr of threads = " + Thread.activeCount());
			System.out.println("Used Memory: "+ ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024*1024)));
			System.out.println("------------------------------------------------");
			System.out.println();

			if (printWriter != null) {
				printWriter.println(timeStamp + ";"
						+ nrOfProcessInstances + ";"
						+ nrOfExecutions + ";"
						+ nrOfTasks + ";"
						+ nrOfAsyncJobs + ";"
						+ nrFinishedProcessInstances + ";"
						+ nrFinishedTasks + ";"
						+ nrCompletedHistoricActivities);
				printWriter.flush();
			}

			if (nrFinishedProcessInstances == numberOfProcessInstances) {
				System.out.println("Conditions for stopping are met");
				allDone = true;
				processEngine.close();
			}
			
			if ( numberOfProcessInstances < 0 || (numberOfProcessInstances > 0 && !allDone) ) {
				try {
					Thread.sleep(10000L);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}


		

		System.out.println("All process instances finished.");

		Date startTime = processEngine.getHistoryService().createHistoricActivityInstanceQuery()
				.orderByHistoricActivityInstanceStartTime().asc().listPage(0, 1).get(0).getStartTime();
		Date endTime = processEngine.getHistoryService().createHistoricActivityInstanceQuery()
				.orderByHistoricActivityInstanceEndTime().desc().listPage(0, 1).get(0).getStartTime();
		long diff = endTime.getTime() - startTime.getTime();
		System.out.println("Time = " + diff + " ms");
		double avg = (double) diff / (double) numberOfProcessInstances;
		System.out.println("Avg time = " + avg + " ms");
		double throughput = 1000.0 / avg;
		System.out.println("Throughput = " + throughput + " process instances / second");

		int nrOfAsyncStepsInProcess = numberOfProcessInstances * 27; // 27 async jobs in process
		System.out.println("Number of executed async jobs = " + nrOfAsyncStepsInProcess);
		double jobsThroughput = 1000.0/ ((double) diff / (double) nrOfAsyncStepsInProcess);
		System.out.println("Throughput = " + jobsThroughput + " jobs / second");


	}


}
