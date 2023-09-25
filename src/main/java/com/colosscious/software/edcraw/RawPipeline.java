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
package com.colosscious.software.edcraw;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


import jdk.internal.org.jline.utils.Log;

//{package};

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class RawPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(RawPipeline.class);
  public interface MyOptions extends PipelineOptions {
	    @Description("Input for the pipeline")
	    @Default.String("nothing")
	    String getInput();
	    void setInput(String input);

	    String getOutput();
	    void setOutput(String output);
	    
	    @Description("Set the runner")
	    @Default.String("測試 CLI options")
	    String getMyPersonalOptions();
	    void setMyPersonalOptions(String personalOptions);
	}
  public static void main(String[] args) {
    PipelineOptionsFactory.register(MyOptions.class);
    MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                  .withValidation()                                       
                                                  .as(MyOptions.class);
    Pipeline p = Pipeline.create(options);
    LOG.info(options.getMyPersonalOptions());
    Map<String, Object> kafkaConfig = new HashMap<String, Object>();
    kafkaConfig.put("group.id", "beam-kafka-g1");

    System.out.println("Start to connect to Kafka Server");
    LOG.info("Start to connect to Kafka Server");
    PCollection<ArrayList<String>> arrayFromKafka = p.apply(KafkaIO.<String, String>read()
    	      .withBootstrapServers("localhost:29092")
    	      .withTopic("demo")  // use withTopics(List<String>) to read from multiple topics.
    	      //.withKeyDeserializerAndCoder(StringDeserializer.class, StringUtf8Coder.of())
    	      //.withValueDeserializerAndCoder(StringDeserializer.class, StringUtf8Coder.of()) 
    	      .withKeyDeserializer(StringDeserializer.class)
    	      .withValueDeserializer(StringDeserializer.class)
    	      
    	      // Above four are required configuration. returns PCollection<KafkaRecord<Long, String>>

    	      // Rest of the settings are optional :

    	      // you can further customize KafkaConsumer used to read the records by adding more
    	      // settings for ConsumerConfig. e.g :
    	      .withConsumerConfigUpdates(kafkaConfig)
    	      // set event times and watermark based on 'LogAppendTime'. To provide a custom
    	      // policy see withTimestampPolicyFactory(). withProcessingTime() is the default.
    	      // Use withCreateTime() with topics that have 'CreateTime' timestamps.
    	      //.withLogAppendTime()

    	      // restrict reader to committed messages on Kafka (see method documentation).
    	      .withReadCommitted()

    	      // offset consumed by the pipeline can be committed back.
    	      .commitOffsetsInFinalize()
    	      .withMaxNumRecords(5)
    	      .withoutMetadata()
    	      // Specified a serializable function which can determine whether to stop reading from given
    	      // TopicPartition during runtime. Note that only {@link ReadFromKafkaDoFn} respect the
    	      // signal.
    	      //.withCheckStopReadingFn(new SerializedFunction() {})
    		)
    .apply(ParDo.of( new DoFn<KV<String,String>, ArrayList<String>>(){
    	
    	@ProcessElement
    	public void printRecord(ProcessContext c) throws JsonMappingException, JsonProcessingException {
//    		System.out.println(c.element().getValue());
//    		String measRecord = c.element().getValue();
    		Map<String, Object> mapping = new ObjectMapper()
    				.readValue(c.element().getValue(), HashMap.class);
//    		System.out.println(measRecord);
    
    		ArrayList<String> res = new ArrayList<String>();
    		res.add(mapping.get("battery_id").toString());
    		res.add(mapping.get("test").toString());
    		res.add(mapping.get("value").toString());
    		res.add(mapping.get("update_dtt").toString());
    		c.output(res);
    	}
    }));
    
    PDone trans1 = arrayFromKafka.apply(JdbcIO.<ArrayList<String>>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                    .create("org.postgresql.Driver", "jdbc:postgresql://127.0.0.1:5432/demodb01")
                    .withUsername("demodb01")
                    .withPassword("demodb01"))
            .withStatement(String.format("INSERT INTO pharmquer.edc_battery (battery_id, test, value, update_dtt) "
            		+ "VALUES (?, ?, ?, ?)"))
            .withPreparedStatementSetter((element, statement) -> {
                statement.setString(1, element.get(0));
                statement.setString(2, element.get(1));
                statement.setDouble(3, Double.parseDouble(element.get(2)));
                statement.setTimestamp(4, parseTimestampString(element.get(3)));
            }));

    @UnknownKeyFor @NonNull @Initialized PCollection<String> trans2 = arrayFromKafka.apply(ParDo.of(new DoFn<ArrayList<String>, String>(){
    	@ProcessElement
    	public void testBranching(ProcessContext c) {
    		LOG.info("A branching result：");
    		System.out.println(c.element());
    	}
    }));
    
    p.run();
    LOG.info(">>>>> Process done. <<<<<");
    System.out.println(">>>>> Process done. Exit.<<<<<");
  }
  public static Timestamp parseTimestampString(String timestr) throws ParseException {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX");
      Date date = dateFormat.parse(timestr);
      long timestamp = date.getTime();
      LOG.info("轉換時間格式: " + timestr + " -> " + timestamp);
      return  new Timestamp(timestamp);
  }
  
  

}
