package pt.sonae.bit.consumer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.HeadersMapExtractAdapter;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.util.GlobalTracer;
import io.opentracing.References;
import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.JaegerTracer;

import org.apache.kafka.clients.consumer.Consumer;
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

        //JavaDStream<ConsumerRecord<String, String>> message =
        stream.map(
               record -> {
                    /*try (Scope scope = tracer.buildSpan("consumption-operation").startActive(true)){
                        System.out.println("#############");
                        for  (Header header : record.headers())
                            System.out.println(header.key() + " -> " + new String(header.value()));

                        return new Tuple2<>(record.key(), record.value());
                    }*/
                    /*
                    final SpanContext context =
                            TracingKafkaUtils.extractSpanContext(record.headers(), tracer);

                    Iterator<Map.Entry<String,String>> iterator = context.baggageItems().iterator();

                    while (iterator.hasNext())
                        System.out.println(iterator.next().getKey() + " --> " + iterator.next().getValue());

                    System.out.println(record.value());

                    System.out.println("############# SPAN Context");
                    for  (Header header : record.headers())
                        System.out.println(header.key() + " -> " + new String(header.value()));
                    */
                    printHeaders(record.headers());

                    return new Tuple2<>(record.key(), record.value());

                }).print();

        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();

    }

    public static void printHeaders(Headers headers){

        //final Tracer tracer = initTracer("kafka-consumer-api");
        //GlobalTracer.register(tracer);
        String serviceName = System.getenv("JAEGER_SERVICE_NAME");
        if (null == serviceName || serviceName.isEmpty()) {
            serviceName = "kafka-internal-serviceName";
        }
        System.setProperty("JAEGER_SERVICE_NAME", serviceName);
        Tracer tracer = Configuration.fromEnv().getTracer();

        try (Scope ignored = tracer.buildSpan("print-headers").startActive(true) ){

            final Map<String, String> map = new HashMap<>();

            for (Header header : headers)
                map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));

            // creates
            SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new MyHeadersMapExtractAdapter(headers, false));
            Span spanPrintHeaders = tracer.buildSpan("spark-consumer").addReference(References.CHILD_OF, spanContext).start();

            //propagate all baggageItems to tags
            Iterator<Map.Entry<String, String>> itemsIterator = spanPrintHeaders.context().baggageItems().iterator();

            while(itemsIterator.hasNext()) {
                Map.Entry<String, String> currEntry = itemsIterator.next();
                spanPrintHeaders.setTag(currEntry.getKey(),currEntry.getValue());
            }


           spanPrintHeaders.finish();


            for (Header header : headers)
                System.out.println(header.key() + " -> " + new String(header.value()));


       }
    }


    public static JaegerTracer initTracer(String service) {
        SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv().withType("const").withParam(1);
        ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true);
        Configuration config = new Configuration(service).withSampler(samplerConfig).withReporter(reporterConfig);
        return config.getTracer();
    }
}
