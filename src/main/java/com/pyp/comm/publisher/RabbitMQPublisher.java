package com.pyp.comm.publisher;

import java.io.IOException;

import com.pyp.comm.util.Constants;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQPublisher implements Publisher {
	
	private static RabbitMQPublisher instance = null;
	
	private ConnectionFactory _factory = null;
	private Connection _connection = null;
	private Channel _channel = null;
	
	private RabbitMQPublisher() {		
	}
	
	public static RabbitMQPublisher getInstance() {
		if (null != instance) {
			return instance;
		} 
		
		instance = new RabbitMQPublisher();
				
		return instance;
	}
    
    public void emit() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        //factory.setHost("192.168.99.102");  <--- Previous Docker RabbitMQ container host IP address from "docker-machine ip default"
        // Docker-machine not supported anymore in Mac
        factory.setHost(Constants.LOCALHOST);
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(Constants.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            
            // Publishing to FANOUT Exchange 1
            // QUEUE1 and QUEUE2 should get messages on the Receiver side
            for (int i = 0; i < 10; i++) {
                String message = "Helloworld message - " + i;
                channel.basicPublish(Constants.EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }
            
            // Publishing to DIRECT Exchange 2 (it uses PRED3 key route)
            // QUEUE3 should get message
            channel.exchangeDeclare(Constants.EXCHANGE_NAME2, BuiltinExchangeType.DIRECT);

            for (int i = 11; i < 20; i++) {
                String message = "Helloworld message - " + i;
                channel.basicPublish(Constants.EXCHANGE_NAME2, "PRED3", null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }            
        }      
    }
    
    public void emit(String msg) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        //factory.setHost("192.168.99.102");  <--- Previous Docker RabbitMQ container host IP address from "docker-machine ip default"
        // Docker-machine not supported anymore in Mac
        factory.setHost(Constants.LOCALHOST);
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
        	
        	if (Constants.SUB_TO_EXC_CMD.equalsIgnoreCase(msg)) {
        		channel.exchangeDeclare(Constants.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        		channel.basicPublish(Constants.EXCHANGE_NAME, "PRED1", null, msg.getBytes("UTF-8"));
        	}
        	
        	if (Constants.UNSUB_TO_EXC_CMD.equalsIgnoreCase(msg)) {
        		channel.exchangeDeclare(Constants.EXCHANGE_NAME2, BuiltinExchangeType.DIRECT);
        		channel.basicPublish(Constants.EXCHANGE_NAME2, "PRED3", null, msg.getBytes("UTF-8"));
        	}            
            
        }    	
    }
    
    public void initPublisher() throws Exception {
    	if (null == _factory) {
    		_factory = new ConnectionFactory();
    		_factory.setHost(Constants.LOCALHOST);
    	}
    	
    	if (null == _connection) {
    		_connection = _factory.newConnection();
    	}
    	
    	if (null == _channel) {
    		_channel = _connection.createChannel();
    	}
    }
        
    // REMOVE
    public void pubToQueue(String exg, String routingKey, String msg) throws Exception {
    	_channel.exchangeDeclare(exg, BuiltinExchangeType.DIRECT);
   		_channel.basicPublish(exg, routingKey, null, msg.getBytes("UTF-8"));
    }
    
    public void pubTargetEvRequest(String targetEvRequest) throws Exception {
    	_channel.exchangeDeclare(Constants.TARGET_EVALUATION_EXCHANGE, BuiltinExchangeType.FANOUT);
   		_channel.basicPublish(Constants.TARGET_EVALUATION_EXCHANGE, "ALL", null, targetEvRequest.getBytes("UTF-8"));
    }
 }