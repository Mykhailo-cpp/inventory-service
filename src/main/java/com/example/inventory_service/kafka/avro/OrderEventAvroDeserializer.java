package com.example.inventory_service.kafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OrderEventAvroDeserializer implements Deserializer<SpecificRecord> {
    @Override
    public SpecificRecord deserialize(String topic, byte[] data) {
        throw new SerializationException("Use deserialize(topic, headers, data)");
    }
    @Override
    public SpecificRecord deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) return null;
        Header h = headers.lastHeader(OrderEventTypes.TYPE_HEADER);
        if (h == null) throw new SerializationException("Missing __TypeId__ header");
        try {
            Class<? extends SpecificRecord> type =
                    OrderEventTypes.classOf(new String(h.value(), StandardCharsets.UTF_8));
            SpecificData model = SpecificData.getForClass(type);
            Schema schema = model.getSchema(type);
            SpecificDatumReader<SpecificRecord> reader = new SpecificDatumReader<>(schema, schema, model);
            return reader.read(null, DecoderFactory.get().binaryDecoder(data, null));
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Can't deserialize Avro message", e);
        }
    }
}
