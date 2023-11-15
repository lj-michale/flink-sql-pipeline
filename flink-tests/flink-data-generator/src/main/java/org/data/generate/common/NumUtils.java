package org.data.generate.common;

import java.util.Random;


/**
 * @descri:
 *
 * @author: lj.michale
 * @date: 2023/11/15 15:44
 */
public class NumUtils {
    public static int Num(){
        Random random = new Random();
        int a = random.nextInt(10) + 1;

        return a;
    }

}
