package pt.sonae.bit.consumer;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import io.opentracing.SpanContext;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.util.GlobalTracer;
import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.JaegerTracer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

public class KafkaConsumer {

    public static void main( String[] args ) throws InterruptedException {

        /*GlobalTracer.register(
                new Configuration(
                        "your_service_name",
                        new Configuration.SamplerConfiguration("const", 1),
                        new Configuration.ReporterConfiguration(
                                false, "localhost", null, 1000, 10000)
                ).getTracer());*/

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]") // local mode with 2 threads
                .setAppName("KafkaConsumer");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(10 * 1000L));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.188.101:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "consumer-spark");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topicsSet = Arrays.asList("queuing_messages_events");

        // Create direct kafka stream with brokers and topics

        ConsumerStrategy<String, String> consumerStrategy = ConsumerStrategies.Subscribe(topicsSet, kafkaParams);

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        consumerStrategy
                );

        //stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        stream.map(
                record -> {
                    System.out.println("#############");


                    for  (Header header : record.headers())
                        System.out.println(header.key() + " -> " + header.value());

                    return new Tuple2<>(record.key(), record.value());
                }
                );


        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();

    }

    public static JaegerTracer initTracer(String service) {
        SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv().withType("const").withParam(1);
        ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true);
        Configuration config = new Configuration(service).withSampler(samplerConfig).withReporter(reporterConfig);
        return config.getTracer();
    }
}
