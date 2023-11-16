package org.turing.common.toolkit;

import lombok.SneakyThrows;

/**
 * @descri:  线程池工具类
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:43
 */
public final class ThreadUtil {

    /**
     * 睡眠当前线程指定时间 {@param millis}
     *
     * @param millis 睡眠时间，单位毫秒
     */
    @SneakyThrows(value = InterruptedException.class)
    public static void sleep(long millis) {
        Thread.sleep(millis);
    }

}