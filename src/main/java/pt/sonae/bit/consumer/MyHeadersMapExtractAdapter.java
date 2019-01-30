package pt.sonae.bit.consumer;

import io.opentracing.propagation.TextMap;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;


public class MyHeadersMapExtractAdapter implements TextMap {

    private final Map<String, String> map = new HashMap<>();

    MyHeadersMapExtractAdapter(Headers headers, boolean second) {
        for (Header header : headers) {
            if (second) {
                if (header.key().startsWith("second_span_")) {
                    map.put(header.key().replaceFirst("^second_span_", ""),
                            new String(header.value(), StandardCharsets.UTF_8));
                }
            } else {
                map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        }
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException(
                "MyHeadersMapExtractAdapter should only be used with Tracer.extract()");
    }
}