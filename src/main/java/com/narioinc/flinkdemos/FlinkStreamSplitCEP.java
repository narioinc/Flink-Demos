package com.narioinc.flinkdemos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.PatternProcessFunction.Context;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

public class FlinkStreamSplitCEP {


	static int counter = 0;
	Set<String> deviceSet = new HashSet<String>();
	public static void main(String[] args) throws Exception {
		// create execution environment

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "flink_consumer");


		FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<ObjectNode>("source-topic", new JSONKeyValueDeserializationSchema(false), properties);
		consumer.setStartFromLatest();
		consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ObjectNode>() {
		
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Watermark getCurrentWatermark() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public long extractTimestamp(ObjectNode element, long previousElementTimestamp) {
				//System.out.println(previousElementTimestamp + " " + element);
				return 0;
			}
		});

		DataStream<ObjectNode> stream = env.addSource(consumer);
		
		DataStream<ObjectNode> deviceTypeA = stream.filter(new FilterFunction<ObjectNode>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(ObjectNode value) throws Exception {
				if (value.get("value").get("deviceType").asText().contentEquals("A"))
					return true;
				else
					return false;
			}
		});
		
		SplitStream<ObjectNode> splitStream = deviceTypeA.split(new OutputSelector<ObjectNode>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> select(ObjectNode value) {
				// TODO Auto-generated method stub
				List<String> output = new ArrayList<String>();
				output.add(value.get("value").get("deviceID").asText());
				return output;
			}
		});
		
		DataStream<ObjectNode> device001Stream = splitStream.select("1");
		device001Stream.print();
		
		AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
		
		Pattern<ObjectNode, ?> pattern = Pattern.<ObjectNode>begin("start", skipStrategy).where(new SimpleCondition<ObjectNode>() {

			@Override
			public boolean filter(ObjectNode value) throws Exception {
				// TODO Auto-generated method stub
				return (Integer.parseInt(value.get("value").get("temp").asText()) < 21);
			}
		}).times(2).within(Time.minutes(1));

		PatternStream<ObjectNode> patternStream = CEP.pattern(device001Stream, pattern);

		DataStream<String> result = patternStream.process(
				new PatternProcessFunction<ObjectNode, String>() {

					/**
					 * 
					*/
					private static final long serialVersionUID = 1L;

					@Override
					public void processMatch(Map<String, List<ObjectNode>> match, Context ctx, Collector<String> out) throws Exception {
						// TODO Auto-generated method stub
						System.out.println("here is the match :");
						 for (ObjectNode node : match.get("start")) {  
							 System.out.print(node.get("value").get("temp") + " ");
					     }
					}
				});

		env.execute();
	}



}
