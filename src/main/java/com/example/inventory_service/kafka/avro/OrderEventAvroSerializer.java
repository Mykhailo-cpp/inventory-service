package com.example.inventory_service.kafka.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class OrderEventAvroSerializer implements Serializer<SpecificRecord> {
    @Override
    public byte[] serialize(String topic, SpecificRecord data) {
        throw new SerializationException("Use serialize(topic, headers, data)");
    }
    @Override
    public byte[] serialize(String topic, Headers headers, SpecificRecord data) {
        if (data == null) return null;
        try {
            headers.remove(OrderEventTypes.TYPE_HEADER);
            headers.add(OrderEventTypes.TYPE_HEADER,
                    OrderEventTypes.nameOf(data.getClass()).getBytes(StandardCharsets.UTF_8));
            SpecificDatumWriter<SpecificRecord> writer =
                    new SpecificDatumWriter<>(data.getSchema(), SpecificData.getForClass(data.getClass()));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Can't serialize " + data.getClass().getSimpleName(), e);
        }
    }
}
