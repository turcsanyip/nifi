package org.apache.nifi.services.kafka;

import org.apache.nifi.controller.AbstractControllerService;

import java.util.HashMap;
import java.util.Map;

public class KafkaTransactionContextControllerService extends AbstractControllerService implements KafkaTransactionContextService {

    private final Map<String, KafkaTransactionContext> contextMap = new HashMap<>();

    @Override
    public void addTransactionContext(String id, KafkaTransactionContext context) {
        contextMap.put(id, context);
    }

    @Override
    public KafkaTransactionContext getTransactionContext(String id) {
        return contextMap.get(id);
    }

    @Override
    public void removeTransactionContext(String id) {
        contextMap.remove(id);
    }
}
