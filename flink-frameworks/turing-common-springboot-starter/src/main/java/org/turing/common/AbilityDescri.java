package org.turing.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @descri:
 *
 * @author: lj.michale
 * @date: 2023/11/16 14:56
 */

@Target({ElementType.TYPE,ElementType.METHOD,ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AbilityDescri {

    public String desc() default "用途描述";

    public String value() default "属性值";

}
