package com.example.thrift.server;

import com.example.thrift.generated.MessageProcessor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MessageProcessorImpl implements MessageProcessor.Iface {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessorImpl.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public String processMessage(String message) throws TException {
        logger.info("Received message: {}", message);

        String timestamp = LocalDateTime.now().format(formatter);
        String processed = String.format("[%s] Processed: %s (length: %d, uppercase: %s)",
                timestamp,
                message,
                message.length(),
                message.toUpperCase());

        logger.info("Returning processed message: {}", processed);
        return processed;
    }
}
