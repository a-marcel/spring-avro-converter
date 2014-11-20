package com.weeaar.spring.http.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

public class AvroConverter
{
	public static <T> T convertFromJson(Object json, Schema schema, Class<T> className) throws IOException
	{
		T returnObject = null;
		
		BinaryDecoder jsonDecoder;
		
		if (json instanceof InputStream)
		{
			jsonDecoder = DecoderFactory.get().binaryDecoder((InputStream)json, null);			
		}
		else
		{
			byte[] jsonBytes = json.toString().getBytes();
			
			jsonDecoder = DecoderFactory.get().binaryDecoder(jsonBytes, null);			
		}

		SpecificDatumReader<T> reader = new SpecificDatumReader<T>(schema);
		returnObject = reader.read(null, jsonDecoder);

		return returnObject;
	}
	
	public static <T> byte[] convertToJson(Object value, Schema raw) throws IOException
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		GenericDatumWriter<T> writer = new GenericDatumWriter<T>(raw);

		Encoder encoder = EncoderFactory.get().binaryEncoder(bos, null);

		writer.write((T) value, encoder);
		encoder.flush();

		return bos.toByteArray();
	}
}
