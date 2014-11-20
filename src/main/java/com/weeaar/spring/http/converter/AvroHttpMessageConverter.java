package com.weeaar.spring.http.converter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

public class AvroHttpMessageConverter<T> extends AbstractHttpMessageConverter<Object>
{
	protected final Log logger = LogFactory.getLog(getClass());

	public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

	/*
	 * public static AvroHttpMessageConverter getMapper() { return new
	 * AvroHttpMessageConverter(); }
	 */

	public AvroHttpMessageConverter()
	{
		super();
		List<MediaType> supportedMediaTypes = new ArrayList<MediaType>();
		// supportedMediaTypes.add( new MediaType("application", "json",
		// DEFAULT_CHARSET));
		// supportedMediaTypes.add( new MediaType("application", "*+json",
		// DEFAULT_CHARSET));
		supportedMediaTypes.add(new MediaType("application", "bson", DEFAULT_CHARSET));

		this.setSupportedMediaTypes(supportedMediaTypes);
	}

	protected boolean supports(Class<?> clazz)
	{
		if (SpecificRecord.class.isAssignableFrom(clazz) || String.class.isAssignableFrom(clazz))
		{
			return true;
		}
		return false;
	}

	@Override
	protected Object readInternal(Class<? extends Object> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException
	{
		String className = null;
		Class cls;
		try
		{
			className = inputMessage.getHeaders().get("json__TypeId__").get(0);
			cls = Class.forName(className);
		} catch (ClassNotFoundException e)
		{
			logger.error("Coulnd found class (" + className + ") from header json__TypeId__", e);
			return null;
		}
		Method method;
		Schema raw;
		try
		{
			method = cls.getDeclaredMethod("getClassSchema");
			raw = (Schema) method.invoke(null);
		} catch (NoSuchMethodException | SecurityException e)
		{
			logger.error("Class " + className + " not looks like an AvroObject", e);
			return null;
		} catch (IllegalAccessException e)
		{
			logger.error("Class " + className + " not looks like an AvroObject", e);
			return null;
		} catch (IllegalArgumentException e)
		{
			logger.error("Class " + className + " not looks like an AvroObject", e);
			return null;
		} catch (InvocationTargetException e)
		{
			logger.error("Class " + className + " not looks like an AvroObject", e);
			return null;
		}
		
//		String json = IOUtils.toString(inputMessage.getBody(), DEFAULT_CHARSET);
		

		return (T) AvroConverter.convertFromJson(inputMessage.getBody(), raw, cls.getClass());
	}

	@Override
	protected void writeInternal(Object t, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException
	{
		Schema raw = ((SpecificRecord) t).getSchema();

		byte[] returnObject = AvroConverter.convertToJson(t, raw);

		HttpHeaders headers = outputMessage.getHeaders();

		headers.set("json__TypeId__", t.getClass().getCanonicalName().toString());

		outputMessage.getBody().write(returnObject);
	}
}
