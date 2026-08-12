package at.esque.kafka.handlers;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ProducerWrapper {
    private String clusterId;
    private KafkaProducer<Object, Object> producer;
    private RestService schemaRegistryRestService;

    public ProducerWrapper(String clusterId, KafkaProducer<Object, Object> producer, RestService schemaRegistryRestService) {
        this.producer = producer;
        this.schemaRegistryRestService = schemaRegistryRestService;
        this.clusterId = clusterId;
    }

    public KafkaProducer<Object, Object> getProducer() {
        return producer;
    }

    public void setProducer(KafkaProducer<Object, Object> producer) {
        this.producer = producer;
    }

    public RestService getSchemaRegistryRestService() {
        return schemaRegistryRestService;
    }

    public void setSchemaRegistryRestService(RestService schemaRegistryRestService) {
        this.schemaRegistryRestService = schemaRegistryRestService;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
}
