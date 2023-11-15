package org.data.generate.common;

import java.util.Random;

/**
 * @descri:
 *
 * @author: lj.michale
 * @date: 2023/11/15 15:42
 */
public class TimeUtils {

    public static String Time(){
        String[] year = {"2016","2017","2018","2019","2020","2021","2022",};
        String[] mounth = {"01","02","03","04","05","06","07","08","09","10","11","12",};
        String[] day = {"01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30",};
        Random random = new Random();
        int year1 = random.nextInt(year.length);
        int mounth1 = random.nextInt(mounth.length);
        int day1 = random.nextInt(day.length);

        return year[year1]+"-"+mounth[mounth1]+"-"+day[day1];
    }

}
