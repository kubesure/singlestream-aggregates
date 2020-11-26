package io.kubesure.aggregate.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Util {

	

	public static ParameterTool readProperties() throws Exception {
		//ParameterTool parameterTool = ParameterTool.fromPropertiesFile("prospectstream.properties");
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile
								(Util.class.getClassLoader().getResourceAsStream("stream.properties"));
		return parameterTool;
	}
	
	public static StreamExecutionEnvironment prepareExecutionEnv(ParameterTool parameterTool)
		throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setAutoWatermarkInterval(500l);
		// env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		// env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		return env;
	}
}