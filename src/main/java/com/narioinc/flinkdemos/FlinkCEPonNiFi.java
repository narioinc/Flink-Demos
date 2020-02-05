package com.narioinc.flinkdemos;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.PatternProcessFunction.Context;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacket;
import org.apache.flink.streaming.connectors.nifi.NiFiSource;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;


public class FlinkCEPonNiFi {


	static int counter = 0;
	public static void main(String[] args) throws Exception {
		// create execution environment

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
		        .url("http://127.0.0.1:8080/nifi")
		        .portName("device001-output-port")
		        .requestBatchCount(5)	
		        .buildConfig();

		SourceFunction<NiFiDataPacket> nifiSource = new NiFiSource(clientConfig);


		DataStream<NiFiDataPacket> stream = env.addSource(nifiSource);
		
		AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
		
		Pattern<NiFiDataPacket, ?> pattern = Pattern.<NiFiDataPacket>begin("start", skipStrategy).where(new SimpleCondition<NiFiDataPacket>() {

			@Override
			public boolean filter(NiFiDataPacket value) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(new String(value.getContent()));
				return false;
			}
		}).times(2).within(Time.minutes(1));

		PatternStream<NiFiDataPacket> patternStream = CEP.pattern(stream, pattern);

		DataStream<String> result = patternStream.process(
				new PatternProcessFunction<NiFiDataPacket, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void processMatch(Map<String, List<NiFiDataPacket>> match, Context ctx, Collector<String> out) throws Exception {
						// TODO Auto-generated method stub
						System.out.println("here is the match :");
						 for (NiFiDataPacket packet : match.get("start")) {  
							 //System.out.print(node.get("value").get("temp") + " ");
					     }
					}
				});

		env.execute();
	}

}
