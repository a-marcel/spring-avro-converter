package com.weeaar.spring.http.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AvroConverter
{
	protected final static Log logger = LogFactory.getLog(AvroConverter.class);

	private static Map<String, Class> classCache = new ConcurrentHashMap<String, Class>();
	private static Map<String, Schema> schemaCache = new ConcurrentHashMap<String, Schema>();

	public static Class getClass(String className)
	{
		Class c = classCache.get(className);
		if (c == null)
		{
			try
			{
				c = Class.forName(className);
			} catch (ClassNotFoundException e)
			{
				return null;
			}
			classCache.put(className, c);
		}

		return c;
	}

	public static Schema getSchema(String className)
	{
		Class c = AvroConverter.getClass(className);

		if (null == c)
		{
			return null;
		}
		return AvroConverter.getSchema(c);
	}

	public static Schema getSchema(Class clazz)
	{
		try
		{
			Schema raw = schemaCache.get(clazz.getName());

			Method method = null;
			if (null != raw)
			{
				return raw;
			}
			
			Class<?> current = clazz;
			Field schema = null;
			do
			{
				try
				{
					
					schema = current.getDeclaredField("SCHEMA$");
					raw = (Schema) schema.get(current);
				} catch (Exception e)
				{
				}
			} while ((current = current.getSuperclass()) != null);

			schemaCache.put(clazz.getName(), raw);

			return raw;
		} catch (IllegalArgumentException e)
		{
			logger.error("Class " + clazz.getName() + " not looks like an AvroObject", e);
			return null;
		}

	}

	public static <T> T convertFromJson(Object json, Schema schema, Class<T> className) throws IOException
	{
		T returnObject = null;

		BinaryDecoder jsonDecoder;

		if (json instanceof InputStream)
		{
			jsonDecoder = DecoderFactory.get().binaryDecoder((InputStream) json, null);
		} else
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
