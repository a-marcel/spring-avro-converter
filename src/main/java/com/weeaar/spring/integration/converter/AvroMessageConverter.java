package com.weeaar.spring.integration.converter;

import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

public class AvroMessageConverter extends AbstractMessageHandler
{
	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception
	{
		MessageHeaders headers = message.getHeaders();		
	}
}
