package com.example.thrift.transport;

import com.rabbitmq.client.*;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Custom Thrift transport implementation for RabbitMQ
 * Supports both client and server modes for RPC-style communication
 */
public class RabbitMQTransport extends TTransport {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQTransport.class);

    private static final String EXCHANGE_NAME = "thrift-exchange";
    private static final int RPC_TIMEOUT_SECONDS = 30;

    private final Connection connection;
    private final Channel channel;
    private final String queueName;
    private final boolean isClientMode;

    // Client mode fields
    private String replyQueueName;
    private BlockingQueue<byte[]> responseQueue;
    private String correlationId;

    // Read/Write buffers
    private ByteArrayOutputStream writeBuffer;
    private ByteArrayInputStream readBuffer;

    // Server mode fields
    private AMQP.BasicProperties requestProperties;

    /**
     * Constructor for client mode
     */
    public RabbitMQTransport(String host, int port, String username, String password, String queueName)
            throws IOException, TimeoutException {
        this.queueName = queueName;
        this.isClientMode = true;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);

        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
        this.writeBuffer = new ByteArrayOutputStream();
        this.responseQueue = new ArrayBlockingQueue<>(1);
    }

    /**
     * Constructor for server mode
     */
    public RabbitMQTransport(Channel channel, byte[] requestData, AMQP.BasicProperties properties) {
        this.channel = channel;
        this.connection = null;
        this.queueName = null;
        this.isClientMode = false;
        this.requestProperties = properties;
        this.readBuffer = new ByteArrayInputStream(requestData);
        this.writeBuffer = new ByteArrayOutputStream();
    }

    @Override
    public boolean isOpen() {
        return channel != null && channel.isOpen();
    }

    @Override
    public void open() throws TTransportException {
        if (!isClientMode) {
            return;
        }

        try {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);
            replyQueueName = channel.queueDeclare().getQueue();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                    responseQueue.offer(delivery.getBody());
                }
            };

            channel.basicConsume(replyQueueName, true, deliverCallback, consumerTag -> {});
            logger.info("RabbitMQ client transport opened for queue: {}", queueName);
        } catch (IOException e) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Failed to open RabbitMQ transport", e);
        }
    }

    @Override
    public void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
            logger.info("RabbitMQ transport closed");
        } catch (IOException | TimeoutException e) {
            logger.error("Error closing RabbitMQ transport", e);
        }
    }

    @Override
    public int read(byte[] buf, int off, int len) throws TTransportException {
        if (readBuffer == null) {
            throw new TTransportException(TTransportException.NOT_OPEN, "No data to read");
        }

        int bytesRead = readBuffer.read(buf, off, len);
        if (bytesRead == -1) {
            return 0;
        }
        return bytesRead;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        writeBuffer.write(buf, off, len);
    }

    @Override
    public void flush() throws TTransportException {
        if (isClientMode) {
            flushClient();
        } else {
            flushServer();
        }
    }

    @Override
    public void checkReadBytesAvailable(long numBytes) throws TTransportException {
        // For RabbitMQ transport, we read from in-memory buffers
    }

    @Override
    public void updateKnownMessageSize(long size) throws TTransportException {
        // For RabbitMQ transport, message size is managed internally
    }

    @Override
    public org.apache.thrift.TConfiguration getConfiguration() {
        return null;
    }

    private void flushClient() throws TTransportException {
        try {
            byte[] requestData = writeBuffer.toByteArray();
            writeBuffer.reset();

            correlationId = UUID.randomUUID().toString();

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .replyTo(replyQueueName)
                    .build();

            channel.basicPublish(EXCHANGE_NAME, queueName, props, requestData);
            logger.debug("Sent request with correlation ID: {}", correlationId);

            byte[] response = responseQueue.poll(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response == null) {
                throw new TTransportException(TTransportException.TIMED_OUT, "RPC timeout");
            }

            readBuffer = new ByteArrayInputStream(response);
            logger.debug("Received response for correlation ID: {}", correlationId);

        } catch (IOException | InterruptedException e) {
            throw new TTransportException(TTransportException.UNKNOWN, "Failed to send request", e);
        }
    }

    private void flushServer() throws TTransportException {
        try {
            byte[] responseData = writeBuffer.toByteArray();
            writeBuffer.reset();

            AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                    .correlationId(requestProperties.getCorrelationId())
                    .build();

            channel.basicPublish("", requestProperties.getReplyTo(), replyProps, responseData);
            logger.debug("Sent response for correlation ID: {}", requestProperties.getCorrelationId());

        } catch (IOException e) {
            throw new TTransportException(TTransportException.UNKNOWN, "Failed to send response", e);
        }
    }
}
