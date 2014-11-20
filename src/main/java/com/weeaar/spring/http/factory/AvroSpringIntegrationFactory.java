package com.weeaar.spring.http.factory;

import com.weeaar.spring.integration.support.avro.AvroJsonObjectMapper;

public class AvroSpringIntegrationFactory
{
	public static AvroJsonObjectMapper<Object> getMapper()
	{
		return new AvroJsonObjectMapper<Object>();
	}
}
