package com.cloudcomputing.samza.nycabs.application;

import java.util.List;
import java.util.Map;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;

import com.cloudcomputing.samza.nycabs.AdMatchTask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AdMatchTaskApplication implements TaskApplication {

    // Consider modify this zookeeper address, localhost may not be a good choice.
    // If this task application is executing in slave machine.
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("172.31.88.233:2181");

    // Consider modify the bootstrap servers address. This example only cover one address.
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of(
            "172.31.86.55:9092",
            "172.31.95.143:9092",
            "172.31.88.233:9092"
    );

    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    @Override
    public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {
        // Define a system descriptor for Kafka.
        KafkaSystemDescriptor kafkaSystemDescriptor
                = new KafkaSystemDescriptor("kafka").withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<Map<String, Object>> inputDescriptor
                = kafkaSystemDescriptor.getInputDescriptor("events", new JsonSerde<>());
        KafkaOutputDescriptor<Map<String, Object>> outputDescriptor
                = kafkaSystemDescriptor.getOutputDescriptor("ad-stream", new JsonSerde<>());

        taskApplicationDescriptor.withInputStream(inputDescriptor);
        taskApplicationDescriptor.withOutputStream(outputDescriptor);
        taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new AdMatchTask());
    }
}
