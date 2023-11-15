package org.data.generate.common;

import java.util.Random;


/**
 * @descri: 
 *
 * @author: lj.michale
 * @date: 2023/11/15 15:42
 */
public class PhoneUtils {
    //    IP、状态码、 性别
    // 随机生成手机号11位   1 [3,5,7,8]   0-9
    public static String getTel(){
        Random rd = new Random();
        String tel = "1";
        String[] num2 = {"3","5","7","8"};
        tel = tel + num2[rd.nextInt(4)];

        for (int i=0;i<9;i++){
            int num = rd.nextInt(10);
            tel = tel + num;
        }
        return tel;
    }
}
