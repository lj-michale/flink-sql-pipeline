package org.turing.common.enums;

/**
 * @descri:  标识枚举，非 {@link Boolean#TRUE} 即 {@link Boolean#FALSE}
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:21
 */
public enum FlagEnum {

    /**
     * FALSE
     */
    FALSE(0),

    /**
     * TRUE
     */
    TRUE(1);

    private final Integer flag;

    FlagEnum(Integer flag) {
        this.flag = flag;
    }

    public Integer code() {
        return this.flag;
    }

    public String strCode() {
        return String.valueOf(this.flag);
    }

    @Override
    public String toString() {
        return strCode();
    }
}