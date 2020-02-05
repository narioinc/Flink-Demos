package com.narioinc.flinkdemos.utils;

import java.time.Instant;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

/**
 * Simple Class to generate data
 */
public class SimpleStringGenerator implements SourceFunction<String> {
  private static final long serialVersionUID = 119007289730474249L;
  boolean running = true;
  long i = 0;
  private static String[] devTypes = {"A", "B"};
  
  @Override
  public void run(SourceContext<String> ctx) throws Exception {
    while(running) {
      String packet = "{"
      		+ "\"deviceType\": \"" + getDeviceType() + "\","
      		+ "\"temp\": " + Integer.toString(new Random().nextInt(90) + 10) + ", "
      		+ "\"deviceID\": \"" + Integer.toString(new Random().nextInt(3) + 1) + "\","
      		+ "\"timestamp\": " + Instant.now().toEpochMilli()
      		+ "}";
        //System.out.print(packet);		
  	  ctx.collect(packet);
      //System.out.println("{\"temp\":\""+Integer.toString(new Random().nextInt(90) + 10)+"\"}");
      Thread.sleep(10);
    }
  }
  @Override
  public void cancel() {
    running = false;
  }
  
  private String getDeviceType(){
	  int idx = new Random().nextInt(devTypes.length);
	  String random = (devTypes[idx]);
	  return random;
  }
}
