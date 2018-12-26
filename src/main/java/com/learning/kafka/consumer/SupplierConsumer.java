package com.learning.kafka.consumer;

import com.learning.kafka.listeners.RebalanceListener;
import com.learning.kafka.model.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SupplierConsumer{

        public static void main(String[] args) throws Exception{

                String topicName = "SupplierTopic";
                String groupName = "SupplierTopicGroup";

                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092,localhost:9093");
                props.put("group.id", groupName);
                props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props.put("value.deserializer", "com.learning.kafka.deserializer.SupplierDeserializer");


                KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(props);
                RebalanceListener rebalanceListener = new RebalanceListener(consumer);

                consumer.subscribe(Arrays.asList(topicName), rebalanceListener);

                while (true){
                        ConsumerRecords<String, Supplier> records = consumer.poll(100);
                        for (ConsumerRecord<String, Supplier> record : records){
                                System.out.println("Supplier id= " + String.valueOf(record.value().getID()) + " Supplier  Name = " + record.value().getName() + " Supplier Start Date = " + record.value().getStartDate().toString());
                        }
                }

        }
}
