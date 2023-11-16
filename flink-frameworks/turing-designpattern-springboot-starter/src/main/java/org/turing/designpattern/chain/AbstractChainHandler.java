package org.turing.designpattern.chain;

import org.springframework.core.Ordered;

/**
 * @descri: 抽象业务责任链组件
 *
 * @author: lj.michale
 * @date: 2023/11/16 16:10
 */
public interface AbstractChainHandler<T> extends Ordered {

    /**
     * 执行责任链逻辑
     *
     * @param requestParam 责任链执行入参
     */
    void handler(T requestParam);

    /**
     * @return 责任链组件标识
     */
    String mark();
}