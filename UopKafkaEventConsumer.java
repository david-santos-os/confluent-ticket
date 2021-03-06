package com.outsystems.udp.processing.deliver.kafka;

import com.outsystems.udp.processing.deliver.component.UopKafkaEventDeliveryService;
import com.outsystems.udp.processing.deliver.config.ConfigProps;
import com.outsystems.udp.processing.exception.RetryableRuntimeException;
import com.outsystems.udp.processing.kafka.KafkaConsumerThreadMetadataProvider;
import com.outsystems.udp.processing.metrics.MetricProducer;
import com.outsystems.udp.processing.metrics.MetricUtils;
import com.outsystems.udp.processing.model.DataTypeName;
import com.outsystems.udp.processing.model.UopKafkaEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.outsystems.udp.processing.deliver.utils.KafkaUtils.getConsumerGroupId;
import static com.outsystems.udp.processing.deliver.utils.KafkaUtils.getInTopicName;

public class UopKafkaEventConsumer implements KafkaConsumerThreadMetadataProvider, InitializingBean, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(UopKafkaEventConsumer.class);

    protected final KafkaConsumer<String, UopKafkaEvent> kafkaConsumer;
    private final Map<String, Long> kafkaConsumerThreadMetadata;
    private final ConfigurableApplicationContext context;
    private final ConfigProps configs;
    private final DataTypeName dataTypeName;
    private final UopKafkaEventDeliveryService deliveryService;
    protected final String topic;
    private final MetricProducer metricProducer;

    private boolean retrying = false;

    public UopKafkaEventConsumer(ConfigProps configs,
                                 DataTypeName dataTypeName,
                                 ConfigurableApplicationContext context,
                                 MeterRegistry meterRegistry,
                                 KafkaConsumer<String, UopKafkaEvent> kafkaConsumer,
                                 UopKafkaEventDeliveryService deliveryService) {
        this.topic = getInTopicName(configs);
        this.configs = configs;
        this.dataTypeName = dataTypeName;
        this.context = context;
        this.metricProducer = new MetricProducer(meterRegistry);
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaConsumerThreadMetadata = new ConcurrentHashMap<>();
        this.deliveryService = deliveryService;
    }

    @Override
    public void afterPropertiesSet() {
        metricProducer
            .withCommonTagDataType(dataTypeName.name().toLowerCase())
            .withCommonTagInputTopic(topic);
    }

    @Override
    public void destroy() throws Exception {
        kafkaConsumer.close();
        deliveryService.close();
    }

    public void postPoll() {
        // No op
    }

    @SuppressWarnings("java:S1181")
    public void pollBatch() {
        ConsumerRecords<String, UopKafkaEvent> records = null;
        try {
            records = kafkaConsumer.poll(Duration.ofMillis(configs.getKafkaInPollTimeoutMillis()));
            setTimerRunning();
            incrementTotalDeliveryCounter(records);
            consumeBatch(records);
            removeTimerRunning();
            commit(records);
        } catch (WakeupException e) {
            // noop
            LOGGER.info("Shutting down Kafka consumer");
        } catch (RetryableRuntimeException e) {
            incrementDestinationErrorCounter(records);
            // E.g. Bad request. Log and move on.
            if (!e.isRetryable()) {
                LOGGER.error("Failed to deliver message. Not retryable exception, committing offset.", e);
                handleFailureAndCommit(records, e);
            } else {
                // potentially retryable error. Don't commit the offset. Tell the consumer to get this record again.
                LOGGER.error("Failed to deliver message. Offset not committed.", e);
                if (records != null && !records.isEmpty()) {
                    ConsumerRecord<String, UopKafkaEvent> firstRecord = records.iterator().next();
                    try {
                        kafkaConsumer.seek(new TopicPartition(firstRecord.topic(), firstRecord.partition()), firstRecord.offset());
                        retrying = true;
                    } catch (Exception ex) {
                        incrementDeliveryErrorCounters(records, ex);
                        LOGGER.error("!!!!!!!! Unable to set Kafka Offset. This is likely to be a bug (such as trying" +
                            " to set a negative offset, or read from an incorrect partition, or a multithreading issue). " +
                            " Shutting down application !!!!!!!!!", ex);
                        System.exit(SpringApplication.exit(context));
                    }
                }
                // As it failed we should backoff for a moment before trying again
                try {
                    Thread.sleep(configs.getUopDeliveryBackoffMs());
                } catch (InterruptedException ex) {
                    LOGGER.error("Thread interrupted whilst during backoff");
                    throw new RuntimeException("Unexpected interrupt", ex);
                }
            }
        } catch (Exception ex) {
            incrementDeliveryErrorCounters(records, ex);
            LOGGER.error("Failed to deliver message. Committing offset.", ex);
            commit(records);
        }
    }

    private void incrementTotalDeliveryCounter(ConsumerRecords<String, UopKafkaEvent> records) {
        if (records != null && !retrying) {
            for (ConsumerRecord<String, UopKafkaEvent> record : records) {
                metricProducer
                    .deliveryMessagesTotalCounter()
                    .withCustomerTags(record.value())
                    .withConsumerGroupTag(
                        getConsumerGroupId(configs, record.value().getRuleDestinationId()))
                    .increment();
            }
        }
    }

    private void incrementDeliveryErrorCounters(ConsumerRecords<String, UopKafkaEvent> records, Exception ex) {
        if (records != null) {
            for (ConsumerRecord<String, UopKafkaEvent> record : records) {
                metricProducer
                    .deliveryMessagesErrorCounter()
                    .withCustomerTags(record.value())
                    .increment();

                metricProducer
                    .exceptionsTotalCounter()
                    .withTags(Tag.of(MetricUtils.METRIC_LABEL_EXCEPTION_TYPE, ex.getClass().getName()))
                    .increment();
            }
        }
    }

    private void incrementDestinationErrorCounter(ConsumerRecords<String, UopKafkaEvent> records) {
        if (records != null) {
            var uopKafkaEvent = records.iterator().next().value();
            metricProducer
                .deliveryDestinationErrorCounter()
                .withTags(uopKafkaEvent)
                .increment();
        }
    }

    private void handleFailureAndCommit(ConsumerRecords<String, UopKafkaEvent> records, Exception ex) {
        incrementDeliveryErrorCounters(records, ex);
        commit(records);
    }

    /**
     * Commit offsets returned on the last {@link KafkaConsumer#poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the passed timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link KafkaConsumer#commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link KafkaConsumer#subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link KafkaConsumer#poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link KafkaConsumer#poll(Duration)} call.
     * @throws WakeupException if {@link KafkaConsumer#wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
    */
    @SuppressWarnings("java:S1181")
    private void commit(ConsumerRecords<String, UopKafkaEvent> records) {
        try {
            if (records != null && !records.isEmpty()) {
                kafkaConsumer.commitSync();
            }
        } catch (RebalanceInProgressException ex) {
            LOGGER.warn(
                    "RebalanceInProgressException: The consumer instance is in the middle of a rebalance. Continue to the next poll.\n{}",
                    ex.getMessage());
        } catch (Exception ex) {
            LOGGER.error("!!!!!!!! Unable to commit Kafka Offset. Halting application for safety ", ex);
            System.exit(SpringApplication.exit(context));
        }
    }

    public void shutdown() {
        kafkaConsumer.wakeup();
    }

    public void subscribe() {
        kafkaConsumer.subscribe(List.of(topic));
    }

    public void consumeBatch(ConsumerRecords<String, UopKafkaEvent> consumerRecords) {
        List<UopKafkaEvent> events = new ArrayList<>();
        for (var consumerRecord : consumerRecords) {
            if (consumerRecord == null) {
                throw new NullPointerException("Consumed a null UopKafkaEvent from topic " + topic);
            }
            events.add(consumerRecord.value());
            LOGGER.debug("Processing UopKakfaEvent: {}", consumerRecord.value());
        }
        LOGGER.debug("Consumed {} events from topic {}", events.size(), topic);
        if (!events.isEmpty()) {
            deliveryService.deliverBatch(events);
        }
    }

    private void setTimerRunning() {
        kafkaConsumerThreadMetadata.put(Thread.currentThread().getName(), System.currentTimeMillis());
    }

    private void removeTimerRunning() {
        kafkaConsumerThreadMetadata.remove(Thread.currentThread().getName());
    }

    @Override
    public Set<String> getKafkaConsumerThreadIds() {
        return kafkaConsumerThreadMetadata.keySet();
    }

    @Override
    public Long getStartProcessingTimestamp(String threadId) {
        return kafkaConsumerThreadMetadata.get(threadId);
    }
}
