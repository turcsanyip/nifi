package org.apache.nifi.services.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaTransactionContext {

    private final String consumerGroup;

    private final TopicPartition topicPartition;

    private final OffsetAndMetadata offsetAndMetadata;

    public KafkaTransactionContext(String consumerGroup, TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        this.consumerGroup = consumerGroup;
        this.topicPartition = topicPartition;
        this.offsetAndMetadata = offsetAndMetadata;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public OffsetAndMetadata getOffsetAndMetadata() {
        return offsetAndMetadata;
    }
}
