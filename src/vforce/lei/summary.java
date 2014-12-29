package vforce.lei;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class summary extends Configured implements Tool {
    private static String datePattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})";

    public static class mapper extends MapReduceBase implements Mapper<LongWritable, Text , Text ,Text> {
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String line = value.toString();

            // Parse each line in the json format and save the result to a LinkedHashMap
            LinkedHashMap result = readJson(line);
            Iterator it = result.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry me = (Map.Entry) it.next();
                System.out.println(me.getKey());
                output.collect(new Text(me.getKey().toString()),
                        new Text(me.getValue().toString()));
            }
        }

        /* This function is used to parse the line as one json format record
            Input: string in json format
            Output: a LinkedHashMap including the key:value pair of the json file
         */
        public static LinkedHashMap readJson(String line) {
            LinkedHashMap retVal = new LinkedHashMap<String, String>();

            // Clean the data with replacing all the "" with "
            String newLine = line.replaceAll("\"\"", "\"");

            // Utilize the simple-json to parse all the json file
            JSONParser jsonParser = new JSONParser();

            /* Utilize the container to save the result
                Need to implement the createObjectContainer and
                createArrayContainer methods
             */

            ContainerFactory containerFactory = new ContainerFactory() {
                @Override
                public Map createObjectContainer() {
                    return new LinkedHashMap();
                }

                @Override
                public List creatArrayContainer() {
                    return new LinkedList();
                }
            };

            // After parsing the json string, the result will be saved into a map, then
            // the program can traverse the map to get each key: value pair
            try {
                Map json = (Map) jsonParser.parse(newLine, containerFactory);
                Iterator it = json.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry m1 = (Map.Entry) it.next();
                    String field = m1.getKey().toString();

                    // If the key is the type then collect the type as key and "HEAD"
                    // as value in the mapper
                    if (field.equals("type")) {
                        retVal.put(json.get("type"), "HEAD");
                    } else {
                        // Normalize all the fields
                        if (field.equals("user_agent")) {
                            field = "userAgent";
                        } else if (field.equals("session_id")) {
                            field = "sessionID";
                        } else if (field.equals("created_at") ||
                                field.equals("craetedAt")) {
                            field = "createdAt";
                        }

                        // Handle the values have subfields
                        if (m1.getValue().getClass().equals
                                (java.util.LinkedHashMap.class)) {
                            LinkedHashMap l = (LinkedHashMap) m1.getValue();
                            Iterator i = l.entrySet().iterator();
                            while (i.hasNext()) {
                                Map.Entry m2 = (Map.Entry) i.next();
                                String subField = m2.getKey().toString();

                                // Normalize the itemId
                                if (subField.equals("item_id")) {
                                    subField = "itemId";
                                }

                                // Collect the subfields' type as key and "HEAD" as value for mapper
                                String head = json.get("type") + ":" + m1.getKey().toString();
                                retVal.put(head, "HEAD");

                                // Collect all the subfields' key and value for mapper
                                String key = json.get("type") + ":" +
                                        m1.getKey().toString() + ":" + subField;
                                String value = m2.getValue().toString();
                                retVal.put(key, value);
                            }
                        } else {
                            // Handle the values don't have subfields
                            String key = json.get("type") + ":" + field;
                            String value = m1.getValue().toString();
                            retVal.put(key, value);
                        }
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return retVal;
        }
    }

    public static class reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            String retVal = "";
            int dataType;
            int count = 1;

            String curVal = values.next().toString();

            if(curVal.equals("HEAD")){
                // Count the types
                while(values.hasNext()){ // if the key is the head, only output the count
                    count ++;
                    values.next();
                }
                retVal = " - " + String.valueOf(count);
            }else {
                // Summary the non-tye fields

                /* Get the data type of the values
                1 if they're date
                2 if they're numeric
                3 if they're strings
                 */
                dataType = getDataType(curVal);
                if (dataType == 1) {
                    String min = curVal;
                    String max = curVal;

                    while (values.hasNext()) {
                        curVal = values.next().toString();
                        if (curVal.compareTo(min) < 0) {
                            min = curVal;
                        }

                        if (curVal.compareTo(max) > 0) {
                            max = curVal;
                        }
                        count++;
                    }
                    retVal = " - min: " + min + ", max: " + max + ", count: " + count;
                }

                if(dataType == 2){
                    int min,max;
                    int flagForNum = 1; // 1: numeric 2: identifier 3: categorical
                    int curInt = Integer.parseInt(curVal);
                    double sum =curInt;
                    min = curInt;
                    max = curInt;
                    HashMap hashMapForNum = new HashMap();
                    hashMapForNum.put(curVal,null);
                    while(values.hasNext()){
                        curVal = values.next().toString();
                        dataType = getDataType(curVal);
                        count ++ ;
                        if(dataType == 2 && flagForNum == 1){
                            curInt = Integer.parseInt(curVal);
                            sum = sum + curInt;
                            if(curInt < min){ min = curInt;}
                            if(curInt > max){ max = curInt;}
                        }else{
                            if(hashMapForNum.size() >= 10){
                                flagForNum = 2;
                            }else{
                                flagForNum = 3;
                                hashMapForNum.put(curVal,null);
                            }
                        }
                    }

                    if(flagForNum == 1) {
                        String average = String.format("%.2f", sum * 1.0 / count);
                        retVal = " - min: " + min + ", max: " + max + ", average: " + average
                                + ", count: " + count;
                    }else if(flagForNum == 2){
                        retVal = " - identifier, count: " + count;
                    }else{
                        retVal = hashMapForNum.keySet().toString() + ", count: " + count;
                    }
                }

                if (dataType == 3) {
                    HashMap hashMap = new HashMap();
                    hashMap.put(curVal, 1);
                    int flag = 0; // 1: identifier 0:categorical values;
                    while (values.hasNext()) {
                        curVal = values.next().toString();
                        if (hashMap.size() >= 10) { // It's a identifier
                            count ++;
                            flag = 1;
                        } else { //It's a categorical values;
                            hashMap.put(curVal,null);
                            count ++;
                        }
                    }
                    if(flag == 0){
                        retVal = hashMap.keySet().toString() + ", count: " + count;
                    }else{
                        retVal = " - identifier, count: " + count;
                    }
                }
            }
            output.collect(key, new Text(retVal));
        }

//        public static Calendar getDate(String in){
//            String dateString = in.substring(0,10);
//            String timeString = in.substring(11,19);
//            String[] dateArray = dateString.split("-");
//            String[] timeArray = timeString.split(":");
//
//            int year = Integer.parseInt(dateArray[0]);
//            int month = Integer.parseInt(dateArray[1]);
//            int day = Integer.parseInt(dateArray[2]);
//            int hour = Integer.parseInt(timeArray[0]);
//            int minute = Integer.parseInt(timeArray[1]);
//            int second = Integer.parseInt(timeArray[2]);
//
//            Calendar calendar = new GregorianCalendar();
//            calendar.set(year,month,day,hour,minute,second);
//            return calendar;
//        }

        public static int getDataType(String in) {
        /* retVal
        1: date
        2: number
        3: string
         */

            int retVal = 3;
            boolean isDate = true;
            boolean isNumber = false;

            if (isDate) {
                Pattern p = Pattern.compile(datePattern);
                Matcher m = p.matcher(in);

                if (m.find()) {
                    retVal = 1;
                    return retVal;
                } else {
                    isNumber = true;
                }
            }

            if (isNumber) {
                try {
//                    Double d = Double.parseDouble(in);
                    Long l = Long.parseLong(in);
                    retVal = 2;
                    return retVal;
                } catch (NumberFormatException e) {
                    retVal = 3;
                    return retVal;
                }
            }
            return retVal;
        }

    }

    public int run(String[] args) throws IOException {
        Configuration conf = getConf();
        JobConf jobConf = new JobConf(conf, summary.class);
        jobConf.setJobName("summary");

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setMapperClass(mapper.class);
        jobConf.setReducerClass(reducer.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        // Add multiple files as input of MapReduce program
        FileSystem fs = FileSystem.get(jobConf);
        FileStatus[] statusList = fs.listStatus(new Path(args[1]));
        if (statusList != null) {
            for (FileStatus status : statusList) {
                FileInputFormat.addInputPath(jobConf, status.getPath());
            }
        }
        FileOutputFormat.setOutputPath(jobConf, new Path(args[2]));

        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int retVal = ToolRunner.run(new Configuration(), new summary(), args);
        System.exit(retVal);
    }
}
