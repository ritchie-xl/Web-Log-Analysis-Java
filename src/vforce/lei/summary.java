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

            LinkedHashMap result = readJson(line);
            Iterator it = result.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry me = (Map.Entry) it.next();
                System.out.println(me.getKey());
                output.collect(new Text(me.getKey().toString()),
                        new Text(me.getValue().toString()));
            }
        }

        public static LinkedHashMap readJson(String line) {
            LinkedHashMap retVal = new LinkedHashMap<String, String>();
            String newLine = line.replaceAll("\"\"", "\"");

            JSONParser jsonParser = new JSONParser();
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

            try {
                Map json = (Map) jsonParser.parse(newLine, containerFactory);
                Iterator it = json.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry m1 = (Map.Entry) it.next();
                    String field = m1.getKey().toString();

                    // Normalize all the fields
                    if (field.equals("type")) {
                        retVal.put(json.get("type"), m1.getValue().toString());
                    } else {
                        if (field.equals("user_agent")) {
                            field = "userAgent";
                        } else if (field.equals("session_id")) {
                            field = "sessionID";
                        } else if (field.equals("created_at") ||
                                field.equals("craetedAt")) {
                            field = "createdAt";
                        }

                        if (m1.getValue().getClass().equals
                                (java.util.LinkedHashMap.class)) {
                            LinkedHashMap l = (LinkedHashMap) m1.getValue();
                            Iterator i = l.entrySet().iterator();
                            while (i.hasNext()) {
                                Map.Entry m2 = (Map.Entry) i.next();
                                String subField = m2.getKey().toString();
                                if (subField.equals("item_id")) {
                                    subField = "itemID";
                                }
                                String key = json.get("type") + ":" +
                                        m1.getKey().toString() + ":" + subField;
                                String value = m2.getValue().toString();
                                retVal.put(key, value);
                            }
                        } else {
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

            int count = 1;
            String curValue = values.next().toString();
            int dataType = getDataType(curValue);
            String retVal = "";

            if (dataType == 1) { // The value is a date


            } else if (dataType == 2) { // The value is a numeric
                double sum = 0;
                int min;
                int max;
                int curInt = Integer.parseInt(curValue);
                sum = sum + curInt;
                min = curInt;
                max = curInt;
                while (values.hasNext()) {
                    curInt = Integer.parseInt(values.next().toString());
                    sum = sum + curInt;
                    count = count + 1;
                    if (curInt > max) {
                        max = curInt;
                    }
                    if (curInt < min) {
                        min = curInt;
                    }
                }

                double avg = sum / count;

                retVal = "min: " + min + ", max: " + max + ", average: " + avg + ",count: " + count;
            } else { // The value is a value or a identifier


            }
            output.collect(key, new Text(retVal));
        }

        public static int getDataType(String in) {
        /* retVal
        1: date
        2: number
        3: value
        4: identifier
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
                    Double d = Double.parseDouble(in);
                    retVal = 2;
                    return retVal;
                } catch (NumberFormatException e) {
                    retVal = 3;
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
