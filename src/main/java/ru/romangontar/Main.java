package ru.romangontar;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.slurm.clients.order.Order;
import io.slurm.clients.orderdelivery.OrderDelivery;
import io.slurm.clients.useraddress.UserAddress;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Main {
    public static void main(String[] args)
    {
        Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "order_delivery_join");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.81.253:19092,192.168.81.253:29092,192.168.81.253:39092");
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.81.253:8091");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://192.168.81.253:8091");
        final Serde<Order> orderSerde = new SpecificAvroSerde<>();
        orderSerde.configure(serdeConfig, false);
        final Serde<UserAddress> userAddressSerde = new SpecificAvroSerde<>();
        userAddressSerde.configure(serdeConfig, false);
        final Serde<OrderDelivery> orderDeliverySerde = new SpecificAvroSerde<>();
        orderDeliverySerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, UserAddress> usersAddresses = builder
                .globalTable("user_address", Consumed.with(Serdes.String(), userAddressSerde),
                        Materialized.with(Serdes.String(), userAddressSerde));

        KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderSerde));

        KStream<String, OrderDelivery> ordersDeliveries = orders.join(
                usersAddresses,
                (key, order) -> order.get("user_id").toString(),
                (order, userAddress) -> {
                    OrderDelivery orderDelivery = new OrderDelivery();
                    orderDelivery.put("id", order.get("id").toString());
                    orderDelivery.put("user_id", order.get("user_id").toString());
                    orderDelivery.put("items", order.get("ordered_items"));
                    orderDelivery.put("address",
                            userAddress.getAddressData().getLine1().toString() + " " +
                            userAddress.getAddressData().getLine2().toString() + " " +
                            userAddress.getAddressData().getZip().toString());
                    return orderDelivery;
                }
        );

        ordersDeliveries.to("order_delivery", Produced.with(Serdes.String(), orderDeliverySerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}