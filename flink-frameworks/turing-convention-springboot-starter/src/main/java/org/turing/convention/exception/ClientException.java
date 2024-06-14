package org.turing.convention.exception;

import org.turing.convention.errorcode.BaseErrorCode;
import org.turing.convention.errorcode.IErrorCode;

/**
 * @descri: 客户端异常
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:56
 */
public class ClientException extends AbstractException {

    public ClientException(IErrorCode errorCode) {
        this(null, null, errorCode);
    }

    public ClientException(String message) {
        this(message, null, BaseErrorCode.CLIENT_ERROR);
    }

    public ClientException(String message, IErrorCode errorCode) {
        this(message, null, errorCode);
    }

    public ClientException(String message, Throwable throwable, IErrorCode errorCode) {
        super(message, throwable, errorCode);
    }

    @Override
    public String toString() {
        return "ClientException{" +
                "code='" + errorCode + "'," +
//                "message='" + errorMessage + "'" +
                '}';
    }
}