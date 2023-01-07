package com.pyp.comm;

import java.io.IOException;
import java.util.HashMap;

import com.pyp.comm.util.Constants;
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
    
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	
	private static final String NODE_INIT_TARGET = "INIT";
	private static final String NODE_ID = "1";
	
	private DeliverCallback deliverCallback, deliverCallback2, deliverCallback3;
	
    private String queueName, queueName2, queueName3;
    private int currentSubscribedTarget = -1; // This should later change to an iterable construct of an EvaluableTarget interface,
    										// with a list of SubscribedTargets, including subscribe and unsibscribe methods
    
    private String currentSubscribedQueue = null; // should create a Queue Map with TargetId -> Target Queue Name

    public void receive() throws Exception {
        factory = new ConnectionFactory();
        //factory.setHost("192.168.99.102"); <--- Previous Docker RabbitMQ container host IP address from "docker-machine ip default"
        // Docker-machine not supported anymore in Mac        
        //factory.setUri("amqp://guest:guest@192.168.99.102:");
        
        factory.setHost(Constants.LOCALHOST);
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(Constants.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, Constants.EXCHANGE_NAME, "PRED1");
        
        queueName2 = channel.queueDeclare().getQueue();
        channel.queueBind(queueName2, Constants.EXCHANGE_NAME, "PRED2");
     
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "' on QUEUE1");
            
            if (message.contains(Constants.SUB_TO_EXC_CMD)) {
            	try {
            	    receiveFromNewExg();
            	    
            	} catch (Exception e) {
            		System.out.println("Failed to setup and read from New Exchange..");
            	}
            }

        };
        
        deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "' on QUEUE2");
        };            
        
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
        channel.basicConsume(queueName2, true, deliverCallback2, consumerTag -> {});
                
    }
    
    // Receive 
    public void receiveAtNode() throws Exception {
        factory = new ConnectionFactory();
        //factory.setHost("192.168.99.102"); <--- Previous Docker RabbitMQ container host IP address from "docker-machine ip default"
        // Docker-machine not supported anymore in Mac        
        //factory.setUri("amqp://guest:guest@192.168.99.102:");
        
        factory.setHost(Constants.LOCALHOST);
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(Constants.TARGET_EVALUATION_EXCHANGE, BuiltinExchangeType.FANOUT);
        String targetEvQueue = channel.queueDeclare().getQueue();
        channel.queueBind(targetEvQueue, Constants.TARGET_EVALUATION_EXCHANGE, "ALL");
        
        channel.exchangeDeclare(NODE_INIT_TARGET, BuiltinExchangeType.FANOUT);
        String initTargetQueue = channel.queueDeclare().getQueue();     
        channel.queueBind(initTargetQueue, NODE_INIT_TARGET, "ALL");
        
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback targetEvCallback = (consumerTag, delivery) -> {
            String targetEvaluationRequest = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + targetEvaluationRequest + "' from TARGET EVALUATION EXCHANGE");
            
            try {
            	processTargetEvRequest(targetEvaluationRequest);

            } catch (Exception e) {
            	System.out.println("Failed to setup and read from New Exchange..");
            }

        };
        
        DeliverCallback initTargetCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "' from INIT TARGET QUEUE");
        };            
        
        channel.basicConsume(targetEvQueue, true, targetEvCallback, consumerTag -> {});
        channel.basicConsume(initTargetQueue, true, initTargetCallback, consumerTag -> {});
                
    }
    
    // This method would be the equivalent to the Broker, only declares LsnReqExchange, NO Queues
    // + the listener request queue
    // Nodes would declare each node's queue
    public void receiveBrokerCreation() throws Exception {
        factory = new ConnectionFactory();
        //factory.setHost("192.168.99.102"); <--- Previous Docker RabbitMQ container host IP address from "docker-machine ip default"
        // Docker-machine not supported anymore in Mac        
        //factory.setUri("amqp://guest:guest@192.168.99.102:");
        
        factory.setHost(Constants.LOCALHOST);
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(Constants.TARGET_EVALUATION_EXCHANGE, BuiltinExchangeType.FANOUT);
        channel.exchangeDeclare(NODE_INIT_TARGET, BuiltinExchangeType.FANOUT);
        
        //queueName = channel.queueDeclare().getQueue();
        //channel.queueDeclare(Constants.LSN_QUEUE_NAME_1, false, false, true, new HashMap<String, Object>());
        //channel.queueBind(Constants.LSN_QUEUE_NAME_1, Constants.EXCHANGE_NAME, Constants.LSN_EXG_ROUTING_KEY_1);
        
        //channel.queueDeclare(Constants.LSN_QUEUE_NAME_2, false, false, true, new HashMap<String, Object>());
        //channel.queueBind(Constants.LSN_QUEUE_NAME_2, Constants.EXCHANGE_NAME, Constants.LSN_EXG_ROUTING_KEY_2);        
        
    }
    
    private void processTargetEvRequest(String targetEvRequest)  {
        // Testing behavior
    	
    	if (currentSubscribedTarget >= 0) {
    		unsubscribeToTarget(currentSubscribedTarget, currentSubscribedQueue);
    	}    	
    	
        currentSubscribedTarget = getTargetId(NODE_ID, targetEvRequest);
        currentSubscribedQueue = getNodeQueueName(currentSubscribedTarget);
    	
        subscribeToTarget(targetEvRequest);
    }
    
    private void subscribeToTarget(String targetEvRequest) {
    	try {
    		System.out.println("Attempting to subscribe to Target Exchange [" + targetEvRequest + "] ");

    		// Assuming TRUE target evaluation
    		
            // ===== Second exchange and corresponding queue
            channel.exchangeDeclare(targetEvRequest, BuiltinExchangeType.FANOUT);
             
            
            channel.queueDeclare(currentSubscribedQueue, false, false, true, new HashMap<String, Object>());
            channel.queueBind(currentSubscribedQueue, targetEvRequest, "ALL");            
    		
            System.out.println("Successfully subscribed to Exchange!");
            
            DeliverCallback currentTargetQueueCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "' from " + targetEvRequest);
                
            };                
        	
            channel.basicConsume(currentSubscribedQueue, true, currentTargetQueueCallback, consumerTag -> {});
            
    	} catch (IOException ioe) {
    	
    		System.out.println("Got error while trying to subscribe to exchange: " + ioe.getMessage());
    	}    	
    	
    }
    
    /// TODO: Need to implement queue Name generation based on a hash with target and node id 
    private void unsubscribeToTarget(int _targetId, String _targetQueue) {
    	try {
     	   System.out.println(" [x] Attempting to delete targetQueue [" + _targetQueue + "] now.. ");
     	   channel.queueDelete(_targetQueue);  
     	   System.out.println(" [x] Deleted targetQueue [" + _targetQueue+ "]");
     	   
     	} catch (IOException ioe) {
     		
     		System.out.println("Got error while trying to delete the queue: " + ioe.getMessage());
     	}

    }
    
    private void receiveFromNewExg() throws Exception {
    
    	try {
    		System.out.println("Attempting to subscribe to Exchange...");

            // ===== Second exchange and corresponding queue
            channel.exchangeDeclare(Constants.EXCHANGE_NAME2, BuiltinExchangeType.DIRECT);
             
            queueName3 = channel.queueDeclare().getQueue();
            channel.queueBind(queueName3, Constants.EXCHANGE_NAME2, "PRED3");            
    		
            System.out.println("Successfully subscribed to Exchange!");
            
    	} catch (IOException ioe) {
    	
    		System.out.println("Got error while trying to subscribe to exchange: " + ioe.getMessage());
    	}    	
    	
        deliverCallback3 = (consumerTag, delivery) -> {
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
    	
        channel.basicConsume(queueName3, true, deliverCallback3, consumerTag -> {});
    	
    }
    
    private static final int getTargetId(String nodeId, String targetName) {
    	return nodeId.concat("-").concat(targetName).hashCode();
    }
    
    private static final String getNodeQueueName(int targetId) {
    	return new StringBuilder("qu-").append(targetId).toString();
    }
}