package test;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by cloudera on 12/26/14.
 */
public class getDataType {

    public static void main(String[] in) {

        String date1 = "2013-05-08T08:00:00Z";
        String test2 = "421413243124";
        String test3 = "29536e1";
        System.out.println(get(date1));

//        String date2 = "2013-05-10T23:42:09-08:00";
//        String date3 = "2013-05-10T23:42:13-08:00";
//
//        System.out.println(date1.compareTo(date3));


        System.out.println(get(test2));
        System.out.println(get(test3));

    }

    public static int get(String in){
        /* retVal
        1: date
        2: number
        3: string
         */

        String datePattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})";

        int retVal=3;
        boolean isDate = true;
        boolean isNumber = false;

        if(isDate == true){
            Pattern p = Pattern.compile(datePattern);
            Matcher m = p.matcher(in);

            if(m.find()){
//                String dateString = in.substring(0,10);
//                String timeString = in.substring(11,19);
//                String[] dateArray = dateString.split("-");
//                String[] timeArray = timeString.split(":");
//
//                int year = Integer.parseInt(dateArray[0]);
//                int month = Integer.parseInt(dateArray[1]);
//                int day = Integer.parseInt(dateArray[2]);
//                int hour = Integer.parseInt(timeArray[0]);
//                int minute = Integer.parseInt(timeArray[1]);
//                int second = Integer.parseInt(timeArray[2]);
//
//                Calendar calendar = new GregorianCalendar();
//                calendar.set(year,month,day,hour,minute,second);
////                System.out.println(calendar.toString());
//
//                Calendar first;
//                first = calendar;
//                System.out.println(first.get(1) + " : " + first.get(2));
                retVal = 1;
                return retVal;
            }else{
                isNumber = true;
            }
        }

        if(isNumber == true){
            try {
//                Double d = Double.parseDouble(in);
                Long i =Long.parseLong(in);
//                System.out.println(i);
                retVal = 2;
                return retVal;
            }catch(NumberFormatException e){
                retVal = 3;
                return retVal;

            }
        }
        return retVal;
    }
}
