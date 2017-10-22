package org.ggrittt.process.myprocess;

import java.util.Random;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTask  implements JavaDelegate {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(DefaultTask.class) ;
	
	private final static Random RANDOM = new Random() ;

	public void execute(DelegateExecution execution) {
		int sleep = RANDOM.nextInt(1000) ;
		LOGGER.info("Sleep {} & Execute {}.",sleep,execution);
		try {
			Thread.sleep(sleep);
		} catch (InterruptedException e) {
			LOGGER.warn("Error while sleeping.",e);
		}
	}

}
