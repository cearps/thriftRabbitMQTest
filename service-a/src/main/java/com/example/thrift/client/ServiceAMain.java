package com.example.thrift.client;

import com.example.thrift.generated.CalculatorService;
import com.example.thrift.generated.MessageProcessor;
import com.example.thrift.transport.RabbitMQTransport;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class ServiceAMain {
    private static final Logger logger = LoggerFactory.getLogger(ServiceAMain.class);

    private static final String RABBITMQ_HOST = "localhost";
    private static final int RABBITMQ_PORT = 5672;
    private static final String RABBITMQ_USERNAME = "admin";
    private static final String RABBITMQ_PASSWORD = "admin";
    private static final String CALCULATOR_QUEUE = "calculator";
    private static final String MESSAGE_PROCESSOR_QUEUE = "message-processor";

    public static void main(String[] args) {
        logger.info("Starting Service A (Client)...");
        logger.info("Interactive CLI - Enter commands:");
        logger.info("  calc <num1> <operation> <num2> - Perform calculation (operations: ADD, SUBTRACT, MULTIPLY, DIVIDE)");
        logger.info("  msg <message> - Process a message");
        logger.info("  quit - Exit the application");
        logger.info("");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        boolean running = true;

        while (running) {
            try {
                System.out.print("> ");
                String input = reader.readLine();

                if (input == null || input.trim().isEmpty()) {
                    continue;
                }

                String[] parts = input.trim().split("\\s+", 2);
                String command = parts[0].toLowerCase();

                switch (command) {
                    case "calc":
                        if (parts.length < 2) {
                            System.out.println("Usage: calc <num1> <operation> <num2>");
                            break;
                        }
                        handleCalculate(parts[1]);
                        break;

                    case "msg":
                        if (parts.length < 2) {
                            System.out.println("Usage: msg <message>");
                            break;
                        }
                        handleMessage(parts[1]);
                        break;

                    case "quit":
                    case "exit":
                        running = false;
                        logger.info("Exiting...");
                        break;

                    default:
                        System.out.println("Unknown command: " + command);
                        System.out.println("Available commands: calc, msg, quit");
                }

            } catch (IOException e) {
                logger.error("Error reading input", e);
                running = false;
            }
        }
    }

    private static void handleCalculate(String args) {
        String[] parts = args.split("\\s+");
        if (parts.length != 3) {
            System.out.println("Usage: calc <num1> <operation> <num2>");
            return;
        }

        try {
            double num1 = Double.parseDouble(parts[0]);
            String operation = parts[1].toUpperCase();
            double num2 = Double.parseDouble(parts[2]);

            logger.info("Calling calculator service: {} {} {}", num1, operation, num2);

            RabbitMQTransport transport = new RabbitMQTransport(
                    RABBITMQ_HOST,
                    RABBITMQ_PORT,
                    RABBITMQ_USERNAME,
                    RABBITMQ_PASSWORD,
                    CALCULATOR_QUEUE
            );

            try {
                transport.open();
                TBinaryProtocol protocol = new TBinaryProtocol(transport);
                CalculatorService.Client client = new CalculatorService.Client(protocol);

                double result = client.calculate(num1, num2, operation);
                System.out.println("Result: " + result);
                logger.info("Received result: {}", result);

            } finally {
                transport.close();
            }

        } catch (NumberFormatException e) {
            System.out.println("Error: Invalid number format");
        } catch (TTransportException e) {
            logger.error("Transport error", e);
            System.out.println("Error: Failed to communicate with server - " + e.getMessage());
        } catch (TException e) {
            logger.error("Thrift error", e);
            System.out.println("Error: " + e.getMessage());
        } catch (IOException | TimeoutException e) {
            logger.error("Connection error", e);
            System.out.println("Error: Failed to connect to RabbitMQ - " + e.getMessage());
        }
    }

    private static void handleMessage(String message) {
        try {
            logger.info("Calling message processor service with message: {}", message);

            RabbitMQTransport transport = new RabbitMQTransport(
                    RABBITMQ_HOST,
                    RABBITMQ_PORT,
                    RABBITMQ_USERNAME,
                    RABBITMQ_PASSWORD,
                    MESSAGE_PROCESSOR_QUEUE
            );

            try {
                transport.open();
                TBinaryProtocol protocol = new TBinaryProtocol(transport);
                MessageProcessor.Client client = new MessageProcessor.Client(protocol);

                String response = client.processMessage(message);
                System.out.println("Response: " + response);
                logger.info("Received response: {}", response);

            } finally {
                transport.close();
            }

        } catch (TTransportException e) {
            logger.error("Transport error", e);
            System.out.println("Error: Failed to communicate with server - " + e.getMessage());
        } catch (TException e) {
            logger.error("Thrift error", e);
            System.out.println("Error: " + e.getMessage());
        } catch (IOException | TimeoutException e) {
            logger.error("Connection error", e);
            System.out.println("Error: Failed to connect to RabbitMQ - " + e.getMessage());
        }
    }
}
