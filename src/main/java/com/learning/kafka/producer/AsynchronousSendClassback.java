package com.learning.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsynchronousSendClassback implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("AsynchronousSendClassback :" + recordMetadata.serializedValueSize());
    }
}
