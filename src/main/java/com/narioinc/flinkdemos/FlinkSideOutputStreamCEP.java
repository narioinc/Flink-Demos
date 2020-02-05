package com.narioinc.flinkdemos;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.narioinc.flinkdemos.models.Rule;

public class FlinkSideOutputStreamCEP {


	static int counter = 0;
	Set<String> deviceSet = new HashSet<String>();
	final static String DEVICE_TYPE_SELECTOR = "A";
	final static String DEVICE_ID_SELECTOR = "1";

	public static void main(String[] args) throws Exception {
		// create execution environment

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		Rule rule = new Rule(4, 60000);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "flink_consumer");


		FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<ObjectNode>("source-topic", new JSONKeyValueDeserializationSchema(false), properties);
		consumer.setStartFromLatest();
		consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ObjectNode>() {

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

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(ObjectNode value) throws Exception {
				if (value.get("value").get("deviceType").asText().contentEquals(DEVICE_TYPE_SELECTOR))
					return true;
				else
					return false;
			}
		});

		final OutputTag<ObjectNode> outputTag = new OutputTag<ObjectNode>("device01-output") {

			private static final long serialVersionUID = 1L;
		};

		SingleOutputStreamOperator<ObjectNode> deviceFilteredStream = deviceTypeA.process(new ProcessFunction<ObjectNode, ObjectNode>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void processElement(ObjectNode value, ProcessFunction<ObjectNode, ObjectNode>.Context ctx,
					Collector<ObjectNode> out) throws Exception {
				// TODO Auto-generated method stub
				if(value.get("value").get("deviceID").asText().contentEquals(DEVICE_ID_SELECTOR)) {
					ctx.output(outputTag, value);
				}
			}
		});

		DataStream<ObjectNode> device001Stream = deviceFilteredStream.getSideOutput(outputTag);
		device001Stream.print();

		AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
		Pattern<ObjectNode, ?> pattern = Pattern.<ObjectNode>begin("start", skipStrategy).where(new SimpleCondition<ObjectNode>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(ObjectNode value) throws Exception {
				// TODO Auto-generated method stub
				return (Integer.parseInt(value.get("value").get("temp").asText()) < 21);
			}
		}).times(rule.getOccurence()).within(Time.milliseconds(rule.getDuration()));

		PatternStream<ObjectNode> patternStream = CEP.pattern(device001Stream, pattern);

		DataStream<String> result = patternStream.process(
				new PatternProcessFunction<ObjectNode, String>() {

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
