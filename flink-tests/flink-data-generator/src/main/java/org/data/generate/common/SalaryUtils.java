package org.data.generate.common;

import java.util.Random;

/**
 * @descri: 
 *
 * @author: lj.michale
 * @date: 2023/11/15 15:42
 */
public class SalaryUtils {

    public static String getSalary(){
        String[] salary = {"4500","6500","8000","10000","12000","18000","20000",};
        Random random = new Random();
        int salary1 = random.nextInt(salary.length);

        return salary[salary1]+"元";
    }

}
