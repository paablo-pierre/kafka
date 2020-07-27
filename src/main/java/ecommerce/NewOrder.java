package ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrder {
    public static void main (String args[]) throws ExecutionException, InterruptedException {
        var produce = new KafkaProducer<String, String>(properties());
        var value = "pedido1, 20031011";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
        produce.send(record, (data, ex) -> {
            if(ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + ":::" + data.partition() + "/" + data.offset() + "/" + data.timestamp());
        }).get();
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
