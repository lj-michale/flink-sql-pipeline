package org.turing.designpattern.config;

import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.turing.base.springboot.starter.config.ApplicationBaseAutoConfiguration;
import org.turing.designpattern.chain.AbstractChainContext;
import org.turing.designpattern.strategy.AbstractStrategyChoose;

/**
 * @descri: 设计模式自动装配
 *
 * @author: lj.michale
 * @date: 2023/11/16 16:11
 */
@ImportAutoConfiguration(ApplicationBaseAutoConfiguration.class)
public class DesignPatternAutoConfiguration {

    /**
     * 策略模式选择器
     */
    @Bean
    public AbstractStrategyChoose abstractStrategyChoose() {
        return new AbstractStrategyChoose();
    }

    /**
     * 责任链上下文
     */
    @Bean
    public AbstractChainContext abstractChainContext() {
        return new AbstractChainContext();
    }
}