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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

public class ReadFromKafka {


  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    
    FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("source-topic", new SimpleStringSchema(), properties);
    consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
		
			
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
		public long extractTimestamp(String element, long previousElementTimestamp) {
			System.out.print(previousElementTimestamp + " " + element);
			return 0;
		}
	});
    DataStream<String> stream = env
            .addSource(consumer);

    stream.flatMap(new FlatMapFunction<String, String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public void flatMap(String value, Collector<String> out) throws Exception {
			if(Integer.parseInt(value) > 50) {
				out.collect(value);
				System.out.println("value : " + value);
			}
		}
	});

    env.execute();
  }


}
