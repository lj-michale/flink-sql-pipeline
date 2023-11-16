package org.turing.convention.errorcode;

/**
 * @descri: 平台错误码
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:50
 */
public interface IErrorCode {

    /**
     * 错误码
     */
    String code();

    /**
     * 错误信息
     */
    String message();
}