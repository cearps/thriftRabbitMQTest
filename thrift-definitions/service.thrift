namespace java com.example.thrift.generated

/**
 * Calculator service for basic arithmetic operations
 */
service CalculatorService {
    /**
     * Performs a calculation based on the operation
     * @param num1 First operand
     * @param num2 Second operand
     * @param operation Operation to perform (ADD, SUBTRACT, MULTIPLY, DIVIDE)
     * @return Result of the calculation
     */
    double calculate(1: double num1, 2: double num2, 3: string operation)
}

/**
 * Message processor service for handling text messages
 */
service MessageProcessor {
    /**
     * Processes a message and returns a response
     * @param message The message to process
     * @return Processed message response
     */
    string processMessage(1: string message)
}
