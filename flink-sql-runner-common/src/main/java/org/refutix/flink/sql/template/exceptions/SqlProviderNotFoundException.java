package org.refutix.flink.sql.template.exceptions;

public class SqlProviderNotFoundException extends RuntimeException{

    public SqlProviderNotFoundException() {
        super();
    }

    public SqlProviderNotFoundException(String message) {
        super(message);
    }

    public SqlProviderNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlProviderNotFoundException(Throwable cause) {
        super(cause);
    }
}
