package org.turing.convention.exception;

import org.turing.convention.errorcode.BaseErrorCode;
import org.turing.convention.errorcode.IErrorCode;

import java.util.Optional;

/**
 * @descri: 服务端异常
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:58
 */
public class ServiceException extends AbstractException {

    public ServiceException(String message) {
        this(message, null, BaseErrorCode.SERVICE_ERROR);
    }

    public ServiceException(IErrorCode errorCode) {
        this(null, errorCode);
    }

    public ServiceException(String message, IErrorCode errorCode) {
        this(message, null, errorCode);
    }

    public ServiceException(String message, Throwable throwable, IErrorCode errorCode) {
        super(Optional.ofNullable(message).orElse(errorCode.message()), throwable, errorCode);
    }

    @Override
    public String toString() {
        return "ServiceException{" +
                "code='" + errorCode + "'," +
//                "message='" + errorMessage + "'" +
                '}';
    }
}