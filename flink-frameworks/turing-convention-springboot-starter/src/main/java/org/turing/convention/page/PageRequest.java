package org.turing.convention.page;


import lombok.Data;

/**
 * @descri: 分页请求对象
 *
 * @author: lj.michale
 * @date: 2023/11/16 15:59
 */
@Data
public class PageRequest {

    /**
     * 当前页
     */
    private Long current = 1L;

    /**
     * 每页显示条数
     */
    private Long size = 10L;
}