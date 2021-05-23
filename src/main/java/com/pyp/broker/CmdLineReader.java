package com.pyp.broker;

import java.util.Scanner;

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
					new ReceiveLogs().receive();
					break;
				case "emit":
					new EmitLog().emit();
					break;
				case "msg":
					System.out.println("Enter message: ");
					inputCmd = scanner.nextLine();
					
					new EmitLog().emitMsg(inputCmd);
					break;					
					
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
