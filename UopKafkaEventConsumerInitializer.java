package com.outsystems.udp.processing.deliver.kafka;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.concurrent.atomic.AtomicBoolean;

public class UopKafkaEventConsumerInitializer implements InitializingBean, DisposableBean {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final UopKafkaEventConsumer consumer;

    public UopKafkaEventConsumerInitializer(UopKafkaEventConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void afterPropertiesSet() {
        consumer.subscribe();
        Runnable runnable = () -> {
            while (!closed.get()) {
                consumer.pollBatch(); // this is implemented this way to assist with unit testing
            }
        };
        new Thread(runnable).start();
    }

    @Override
    public void destroy() {
        closed.set(true);
        consumer.shutdown();
    }

}
