package com.pyp.comm;

import java.util.Scanner;

import com.pyp.comm.publisher.RabbitMQPublisher;
import com.pyp.comm.util.Constants;

public class CmdLineReader {
	
	public static void main(String[] args) {
		
		Scanner scanner = new Scanner(System.in);
		String inputCmd = null;
		boolean exit = false;
		
		while (!exit) {
			System.out.println("Give cmd: ");
			inputCmd = scanner.nextLine();

			try {
				switch (inputCmd) {
				case "rcv":
					System.out.println("Receiving messages now..");
					new ReceiveLogs().receive();
					break;

				case "nodercv":
					System.out.println("Receiving messages now..");
					new ReceiveLogs().receiveAtNode();
					break;
										
				case "emit":
					RabbitMQPublisher.getInstance().emit();
					break;
					
				case "msg":
					System.out.println("Enter message: ");
					inputCmd = scanner.nextLine();
					
					RabbitMQPublisher.getInstance().emit(inputCmd);
					break;
					
				case "tgt":
					System.out.println("Enter targetEvRequest: ");
					inputCmd = scanner.nextLine();
					
					RabbitMQPublisher.getInstance().initPublisher();
					RabbitMQPublisher.getInstance().pubTargetEvRequest(inputCmd);
					
					break;					
									
				case "pub":
					// REMOVE
					RabbitMQPublisher.getInstance().initPublisher();
					
					System.out.println("[Exchange]: ");
					String exg = scanner.nextLine();
					
					System.out.println("[Routing key]: ");
					String rk = scanner.nextLine();
					
					System.out.println("[Message]: ");
					String msg = scanner.nextLine();
					
					RabbitMQPublisher.getInstance().pubToQueue(exg, rk, msg);
					break;
					
//				case: "activate":
/*					// Raises a regular node
 * 					 
 */
					
//				case: "run":
/*					// Initialize Node if not done. This includes adding NodeProcessor to ProcessorList
 * 					CommandRunner.executeCommand(inputCmdParams);
 * 					
 */
					
				default:
					exit = true;
				}

			} catch (Exception e) {
				System.out.println("An error occurred: " + e.getMessage());
			}
		
		}
		
		scanner.close();
	}

}
