package org.apache.nifi.services.kafka;

import org.apache.nifi.controller.ControllerService;

public interface KafkaTransactionContextService extends ControllerService {

    void addTransactionContext(String id, KafkaTransactionContext context);

    KafkaTransactionContext getTransactionContext(String id);

    void removeTransactionContext(String id);
}
