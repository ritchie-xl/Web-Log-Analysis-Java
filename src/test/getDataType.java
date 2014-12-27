package test;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by cloudera on 12/26/14.
 */
public class getDataType {

    public static void main(String[] in) {
        /* retval
        1: date
        2: number
        3: value
         */
        String test1 = "2013-05-08T08:00:00Z";
        String test2 = "421413243124";
        String test3 = "b549de69-a0dc-4b8a-8ee1-01f1a1f5a66e";
        System.out.println(get(test1));
        System.out.println(get(test2));
        System.out.println(get(test3));

    }

    public static int get(String in){

        String datePattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})";

        int retVal=1;
        boolean isDate = true;
        boolean isNumber = false;
        boolean isValue = false;

        if(isDate == true){
            Pattern p = Pattern.compile(datePattern);
            Matcher m = p.matcher(in);

            if(m.find()){
                isDate = true;
                retVal = 1;
                return retVal;
            }else{
                isDate = false;
                isNumber = true;
                isValue = false;
            }
        }

        if(isNumber == true){
            try {
                Double d = Double.parseDouble(in);
                retVal = 2;
                return retVal;
            }catch(NumberFormatException e){
                isNumber = false;
                isValue = true;
                isDate = false;
            }
        }

        retVal = 3;
        return retVal;
    }
}
