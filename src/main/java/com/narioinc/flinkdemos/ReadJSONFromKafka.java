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


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import scala.util.parsing.json.JSONObject;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;

public class ReadJSONFromKafka {

  static int counter = 0;
  public static void main(String[] args) throws Exception {
    // create execution environment
	
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    
    FlinkKafkaConsumer<ObjectNode> consumer = new FlinkKafkaConsumer<ObjectNode>("source-topic", new JSONKeyValueDeserializationSchema(false), properties);
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
    stream.flatMap(new FlatMapFunction<ObjectNode, String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public void flatMap(ObjectNode value, Collector<String> out) throws Exception {
			String temp = value.get("value").get("temp").asText();
			if(Integer.parseInt(temp) == 11) {
				out.collect(temp);
				//System.out.println("value : " + value);
			}
		}
	}).timeWindowAll(Time.seconds(15)).reduce(new ReduceFunction<String>() {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String reduce(String value1, String value2) throws Exception {
			System.out.println(counter);
			return Integer.toString(counter++);	
		}
	}).print();

    env.execute();
  }


}
