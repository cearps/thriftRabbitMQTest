package com.example.thrift.server;

import com.example.thrift.generated.CalculatorService;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculatorServiceImpl implements CalculatorService.Iface {
    private static final Logger logger = LoggerFactory.getLogger(CalculatorServiceImpl.class);

    @Override
    public double calculate(double num1, double num2, String operation) throws TException {
        logger.info("Received calculate request: {} {} {}", num1, operation, num2);

        double result;
        switch (operation.toUpperCase()) {
            case "ADD":
                result = num1 + num2;
                break;
            case "SUBTRACT":
                result = num1 - num2;
                break;
            case "MULTIPLY":
                result = num1 * num2;
                break;
            case "DIVIDE":
                if (num2 == 0) {
                    throw new TException("Division by zero");
                }
                result = num1 / num2;
                break;
            default:
                throw new TException("Unknown operation: " + operation);
        }

        logger.info("Returning result: {}", result);
        return result;
    }
}
