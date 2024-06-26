package org.turing.convention.exception;

import org.turing.convention.errorcode.BaseErrorCode;
import org.turing.convention.errorcode.IErrorCode;

/**
 * @descri: 远程服务调用异常
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:57
 */
public class RemoteException extends AbstractException {

    public RemoteException(String message) {
        this(message, null, BaseErrorCode.REMOTE_ERROR);
    }

    public RemoteException(String message, IErrorCode errorCode) {
        this(message, null, errorCode);
    }

    public RemoteException(String message, Throwable throwable, IErrorCode errorCode) {
        super(message, throwable, errorCode);
    }

    @Override
    public String toString() {
        return "RemoteException{" +
                "code='" + errorCode + "'," +
//                "message='" + errorMessage + "'" +
                '}';
    }
}