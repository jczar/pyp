package com.pyp.broker;

import java.io.IOException;

import com.pyp.broker.util.Constants;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * Recieve logs program 
 * @author Ramesh Fadatare
 *
 */
public class ReceiveLogs {
    
    private String queueName, queueName2, queueName3; 

    public void receive() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.99.102");
        //factory.setUri("amqp://guest:guest@192.168.99.102:");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(Constants.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, Constants.EXCHANGE_NAME, "PRED1");
        
        queueName2 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName2, Constants.EXCHANGE_NAME, "PRED2");

        
        // ===== Second exchange and corresponding queue
        channel.exchangeDeclare(Constants.EXCHANGE_NAME2, BuiltinExchangeType.DIRECT);
        queueName3 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName3, Constants.EXCHANGE_NAME2, "PRED3");        

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "' on QUEUE1");
            
            if (message.contains(Constants.SUB_TO_EXC_CMD)) {
            	try {
            		System.out.println("Attempting to subscribe to Exchange...");
            		
                    queueName3 = channel.queueDeclare().getQueue();
                    channel.queueBind(queueName3, Constants.EXCHANGE_NAME2, "PRED3");        
            		
                    System.out.println("Successfully subscribed to Exchange!");
                    
            	} catch (IOException ioe) {
            	
            		System.out.println("Got error while trying to subscribe to exchange: " + ioe.getMessage());
            	}
            }

        };
        
        DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "' on QUEUE2");
        }; 
            

        DeliverCallback deliverCallback3 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "' on QUEUE3");
            
            if (message.contains(Constants.UNSUB_TO_EXC_CMD)) {
            	try {
            	   System.out.println(" [x] Attempting to delete queue3 now.. ");
            	   channel.queueDelete(queueName3);
            	   System.out.println(" [x] Deleted queue3! ");
            	   
            	} catch (IOException ioe) {
            		
            		System.out.println("Got error while trying to delete the queue: " + ioe.getMessage());
            	}
            	
            }            
        };                
        
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
        channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> {});
        
        channel.basicConsume(queueName3, true, deliverCallback3, consumerTag -> {});
    }
}