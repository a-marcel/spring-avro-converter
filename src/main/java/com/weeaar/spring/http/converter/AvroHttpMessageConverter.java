package com.weeaar.spring.http.converter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.AvroRemoteException;
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
	
	protected String HTTP_HEADER_NAME = "json__TypeId__";

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
		if (!inputMessage.getHeaders().containsKey(HTTP_HEADER_NAME))
		{
			return null;
		}

		String className = inputMessage.getHeaders().get(HTTP_HEADER_NAME).get(0);
		Class cls = AvroConverter.getClass(className);

		if (null == cls)
		{
			return null;
		}

		Schema schema = AvroConverter.getSchema(cls);

		if (null == schema)
		{
			return null;
		}
		
		return (T) AvroConverter.convertFromJson(inputMessage.getBody(), schema, cls.getClass());
	}

	@Override
	protected void writeInternal(Object t, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException
	{
		Schema schema = AvroConverter.getSchema(t.getClass());

		byte[] returnObject = AvroConverter.convertToJson(t, schema);

		HttpHeaders headers = outputMessage.getHeaders();

		headers.set(HTTP_HEADER_NAME, t.getClass().getCanonicalName().toString());

		outputMessage.getBody().write(returnObject);
	}

}
