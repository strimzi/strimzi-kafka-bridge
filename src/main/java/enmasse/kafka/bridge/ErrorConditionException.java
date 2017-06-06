package enmasse.kafka.bridge;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;

/**
 * An exception that can be converted into an {@code ErrorCondition}.
 */
class ErrorConditionException extends Exception {

	private static final long serialVersionUID = 887822732457738920L;
	
	private final String error;
	
	public ErrorConditionException(String error, String message, Throwable cause) {
		super(message, cause);
		this.error = error;
	}

	public ErrorConditionException(String error, String message) {
		super(message);
		this.error = error;
	}
	
	/**
	 * Convert this exception into an {@code ErrorCondition}.
	 */
	public ErrorCondition toCondition() {
		return new ErrorCondition(Symbol.getSymbol(this.error), getMessage());
	}

}
