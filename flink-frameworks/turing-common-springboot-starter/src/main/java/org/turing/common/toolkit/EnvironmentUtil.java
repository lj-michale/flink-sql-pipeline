package org.turing.common.toolkit;

import org.springframework.core.env.ConfigurableEnvironment;
import org.turing.base.springboot.starter.ApplicationContextHolder;

import java.util.ArrayList;
import java.util.List;

/**
 * @descri: 环境工具类
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:41
 */
public class EnvironmentUtil {

    private static List<String> ENVIRONMENT_LIST = new ArrayList<>();

    static {
        ENVIRONMENT_LIST.add("dev");
        ENVIRONMENT_LIST.add("test");
    }

    /**
     * 判断当前是否为开发环境
     *
     * @return
     */
    public static boolean isDevEnvironment() {
        ConfigurableEnvironment configurableEnvironment = ApplicationContextHolder.getBean(ConfigurableEnvironment.class);
        String propertyActive = configurableEnvironment.getProperty("spring.profiles.active", "dev");
        return ENVIRONMENT_LIST.stream().filter(each -> propertyActive.contains(each)).findFirst().isPresent();
    }

    /**
     * 判断当前是否为正式环境
     *
     * @return
     */
    public static boolean isProdEnvironment() {
        ConfigurableEnvironment configurableEnvironment = ApplicationContextHolder.getBean(ConfigurableEnvironment.class);
        String propertyActive = configurableEnvironment.getProperty("spring.profiles.active", "dev");
        return !ENVIRONMENT_LIST.stream().filter(each -> propertyActive.contains(each)).findFirst().isPresent();
    }
}