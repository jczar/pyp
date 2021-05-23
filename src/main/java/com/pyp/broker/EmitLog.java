package com.pyp.broker;

import com.pyp.broker.util.Constants;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLog {
    
    public void emit() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.99.102");
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(Constants.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            
            for (int i = 0; i < 10; i++) {
                String message = "Helloworld message - " + i;
                channel.basicPublish(Constants.EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }
            
            channel.exchangeDeclare(Constants.EXCHANGE_NAME2, BuiltinExchangeType.DIRECT);

            for (int i = 11; i < 20; i++) {
                String message = "Helloworld message - " + i;
                channel.basicPublish(Constants.EXCHANGE_NAME2, "PRED3", null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }            
        }      
    }
    
    public void emitMsg(String msg) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.99.102");
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
 }