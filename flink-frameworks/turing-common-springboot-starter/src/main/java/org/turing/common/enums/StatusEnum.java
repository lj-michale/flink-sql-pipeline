package org.turing.common.enums;

/**
 * @descri: 状态枚举
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:23
 */
public enum StatusEnum {

    /**
     * 成功
     */
    SUCCESS(0),

    /**
     * 失败
     */
    FAIL(1);

    private final Integer statusCode;

    StatusEnum(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public Integer code() {
        return this.statusCode;
    }

    public String strCode() {
        return String.valueOf(this.statusCode);
    }

    @Override
    public String toString() {
        return strCode();
    }
}