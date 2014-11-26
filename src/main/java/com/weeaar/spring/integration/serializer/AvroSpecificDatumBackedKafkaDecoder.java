package com.weeaar.spring.integration.serializer;

import java.io.IOException;

import kafka.serializer.Decoder;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.integration.kafka.serializer.avro.AvroDatumSupport;

import com.weeaar.spring.http.converter.AvroConverter;

public class AvroSpecificDatumBackedKafkaDecoder<T> extends AvroDatumSupport<T> implements Decoder<T>
{

	// private final DatumReader<T> reader;
	private final Class<T> clazz;
	private final Schema schema;

	public AvroSpecificDatumBackedKafkaDecoder(final Class<T> specificRecordBase)
	{
		this.clazz = specificRecordBase;
		this.schema = AvroConverter.getSchema(specificRecordBase);
		// this.reader = new SpecificDatumReader<T>(schema);
	}

	@Override
	public T fromBytes(final byte[] bytes)
	{
		T x;
		return AvroConverter.convertFromJson(bytes, schema, this.clazz);
		
//		return x;
		// auch return fromBytes(bytes, reader);
	}
}

/*
 * public class AvroSpecificDatumBackedKafkaDecoder<T> extends
 * AvroDatumSupport<T> implements Decoder<T> { private final DatumReader<T>
 * reader; private final Class<T> clazz; private final Schema schema;
 * 
 * public AvroSpecificDatumBackedKafkaDecoder(final Class<T> specificRecordBase)
 * { this.clazz = specificRecordBase; this.schema =
 * AvroConverter.getSchema(specificRecordBase); this.reader = new
 * SpecificDatumReader<T>(schema); }
 * 
 * @Override public T fromBytes(final byte[] bytes) { BinaryDecoder jsonDecoder
 * = DecoderFactory.get().binaryDecoder(bytes, null); try { return
 * reader.read(null, jsonDecoder); } catch (IOException e) {
 * e.printStackTrace(); } return fromBytes(bytes, reader); } }
 */