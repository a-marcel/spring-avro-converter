package com.weeaar.spring.integration.support.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.type.JavaType;
import org.springframework.integration.support.json.Jackson2JsonObjectMapper;

import com.fasterxml.jackson.core.JsonFactory;
import com.weeaar.spring.http.converter.AvroConverter;

public class AvroJsonObjectMapper<T> extends Jackson2JsonObjectMapper
{
	protected final static Log logger = LogFactory.getLog(AvroJsonObjectMapper.class);

	protected <T> T fromJson(Object json, JavaType type) throws Exception
	{
		Method method = type.getRawClass().getDeclaredMethod("getClassSchema");
		Schema raw = (Schema) method.invoke(null);

		return (T) AvroConverter.convertFromJson(json, raw, type.getRawClass());
	}

	@Override
	public String toJson(Object value) throws Exception
	{
		Schema raw = ((SpecificRecord) value).getSchema();

		byte[] returnObject = AvroConverter.convertToJson(value, raw);

		return returnObject.toString();
	}

	
}