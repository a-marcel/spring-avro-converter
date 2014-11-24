package com.weeaar.spring.integration.serializer;

import kafka.serializer.Encoder;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.integration.kafka.serializer.avro.AvroDatumSupport;

import com.weeaar.spring.http.converter.AvroConverter;

public class AvroSpecificDatumBackedKafkaEncoder<T> extends AvroDatumSupport<T> implements Encoder<T>
{
	private final DatumWriter<T> writer;

	public AvroSpecificDatumBackedKafkaEncoder(final Class<T> specificRecordClazz)
	{
		Schema schema = AvroConverter.getSchema(specificRecordClazz);
		this.writer = new SpecificDatumWriter<T>(schema);
	}

	@Override
	public byte[] toBytes(final T source)
	{
		return toBytes(source, writer);
	}
}
