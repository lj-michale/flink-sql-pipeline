package org.data.generate.common;

import java.util.Random;

/**
 * @descri:
 *
 * @author: lj.michale
 * @date: 2023/11/15 15:40
 */
public class EmailUtils {
    public static String GetEmail(){
        String email = getTel().concat(getCom());
        return email;

    }
    public static String getCom(){
        String[] year = {"@qq.com","@163.com","@yahoo.cn","@sina.com","@gmail.com",};
        Random random = new Random();
        int year1 = random.nextInt(year.length);
        return "."+year[year1];

    }
    public static String getTel(){
        Random rd = new Random();
        String tel;
        String[] num1 = {"2","4","6","8"};
        String[] num2 = {"1","3","5","7"};
        tel = num1[rd.nextInt(4)] + num2[rd.nextInt(4)];

        for (int i=0;i<5;i++){
            int num = rd.nextInt(10);
            tel = tel + num;
        }
        return tel;
    }
}

