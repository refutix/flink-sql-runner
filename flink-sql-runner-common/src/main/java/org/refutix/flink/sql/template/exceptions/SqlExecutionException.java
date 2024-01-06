package org.refutix.flink.sql.template.exceptions;

public class SqlExecutionException extends RuntimeException{

    public SqlExecutionException() {
        super();
    }

    public SqlExecutionException(String message) {
        super(message);
    }

    public SqlExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public SqlExecutionException(Throwable cause) {
        super(cause);
    }
}
