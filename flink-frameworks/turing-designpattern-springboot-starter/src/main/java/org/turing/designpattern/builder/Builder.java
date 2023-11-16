package org.turing.designpattern.builder;

import java.io.Serializable;

/**
 * @descri: Builder 模式抽象接口
 *
 * @author: lj.michale
 * @date: 2023/11/16 16:06
 */
public interface Builder<T> extends Serializable {

    /**
     * 构建方法
     *
     * @return 构建后的对象
     */
    T build();
}