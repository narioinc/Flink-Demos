/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.narioinc.flinkdemos;


import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

public class FlinkCEPOnKafka {

	static int counter = 0;
	public static void main(String[] args) throws Exception {
		// create execution environment

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
				System.out.print(previousElementTimestamp + " " + element);
				return 0;
			}
		});

		DataStream<ObjectNode> stream = env.addSource(consumer);
		Pattern<ObjectNode, ?> pattern = Pattern.<ObjectNode>begin("start").where(new SimpleCondition<ObjectNode>() {

			@Override
			public boolean filter(ObjectNode value) throws Exception {
				// TODO Auto-generated method stub
				return (Integer.parseInt(value.get("value").get("temp").asText()) < 21);
			}
		}).times(2).within(Time.minutes(1));

		PatternStream<ObjectNode> patternStream = CEP.pattern(stream, pattern);

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
