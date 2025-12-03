package com.example.thrift.server;

import com.example.thrift.generated.CalculatorService;
import com.example.thrift.generated.MessageProcessor;
import com.example.thrift.transport.RabbitMQTransport;
import com.rabbitmq.client.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ServiceBMain {
    private static final Logger logger = LoggerFactory.getLogger(ServiceBMain.class);

    private static final String RABBITMQ_HOST = "localhost";
    private static final int RABBITMQ_PORT = 5672;
    private static final String RABBITMQ_USERNAME = "admin";
    private static final String RABBITMQ_PASSWORD = "admin";
    private static final String EXCHANGE_NAME = "thrift-exchange";
    private static final String CALCULATOR_QUEUE = "calculator";
    private static final String MESSAGE_PROCESSOR_QUEUE = "message-processor";

    public static void main(String[] args) {
        logger.info("Starting Service B (Server)...");

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(RABBITMQ_HOST);
            factory.setPort(RABBITMQ_PORT);
            factory.setUsername(RABBITMQ_USERNAME);
            factory.setPassword(RABBITMQ_PASSWORD);

            Connection connection = factory.newConnection();

            setupCalculatorService(connection);
            setupMessageProcessorService(connection);

            logger.info("Service B is ready to accept requests");
            logger.info("Calculator queue: {}", CALCULATOR_QUEUE);
            logger.info("Message Processor queue: {}", MESSAGE_PROCESSOR_QUEUE);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down Service B...");
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.error("Error closing connection", e);
                }
            }));

            Thread.currentThread().join();

        } catch (IOException | TimeoutException | InterruptedException e) {
            logger.error("Failed to start Service B", e);
            System.exit(1);
        }
    }

    private static void setupCalculatorService(Connection connection) throws IOException {
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(CALCULATOR_QUEUE, true, false, false, null);
        channel.queueBind(CALCULATOR_QUEUE, EXCHANGE_NAME, CALCULATOR_QUEUE);
        channel.basicQos(1);

        CalculatorService.Iface calculatorImpl = new CalculatorServiceImpl();
        CalculatorService.Processor<CalculatorService.Iface> processor =
                new CalculatorService.Processor<>(calculatorImpl);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                logger.debug("Received request on calculator queue");

                RabbitMQTransport transport = new RabbitMQTransport(
                        channel,
                        delivery.getBody(),
                        delivery.getProperties()
                );

                TBinaryProtocol protocol = new TBinaryProtocol(transport);
                processor.process(protocol, protocol);

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

            } catch (TException | IOException e) {
                logger.error("Error processing calculator request", e);
            }
        };

        channel.basicConsume(CALCULATOR_QUEUE, false, deliverCallback, consumerTag -> {});
        logger.info("Calculator service listening on queue: {}", CALCULATOR_QUEUE);
    }

    private static void setupMessageProcessorService(Connection connection) throws IOException {
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(MESSAGE_PROCESSOR_QUEUE, true, false, false, null);
        channel.queueBind(MESSAGE_PROCESSOR_QUEUE, EXCHANGE_NAME, MESSAGE_PROCESSOR_QUEUE);
        channel.basicQos(1);

        MessageProcessor.Iface messageProcessorImpl = new MessageProcessorImpl();
        MessageProcessor.Processor<MessageProcessor.Iface> processor =
                new MessageProcessor.Processor<>(messageProcessorImpl);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                logger.debug("Received request on message-processor queue");

                RabbitMQTransport transport = new RabbitMQTransport(
                        channel,
                        delivery.getBody(),
                        delivery.getProperties()
                );

                TBinaryProtocol protocol = new TBinaryProtocol(transport);
                processor.process(protocol, protocol);

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

            } catch (TException | IOException e) {
                logger.error("Error processing message processor request", e);
            }
        };

        channel.basicConsume(MESSAGE_PROCESSOR_QUEUE, false, deliverCallback, consumerTag -> {});
        logger.info("Message Processor service listening on queue: {}", MESSAGE_PROCESSOR_QUEUE);
    }
}
