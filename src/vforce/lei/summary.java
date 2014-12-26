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

public class summary extends Configured implements Tool {
    public static class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable>output, Reporter reporter)
        throws IOException {
            String line = value.toString();

            LinkedHashMap result = readJson(line);
            Iterator it = result.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry me = (Map.Entry)it.next();
                output.collect(new Text(me.getKey().toString()), one);
            }
        }

        public static LinkedHashMap readJson(String line){
            LinkedHashMap retVal = new LinkedHashMap<String, String>();
            String newLine = line.replaceAll("\"\"","\"");

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

            try{
                Map json = (Map)jsonParser.parse(newLine, containerFactory);
                Iterator it = json.entrySet().iterator();
                while(it.hasNext()){
                    Map.Entry m1 = (Map.Entry)it.next();
                    String field = m1.getKey().toString();

                    if(field.equals("type")){
                        retVal.put(json.get("type"), m1.getValue().toString());
                    }else{
                        if(field.equals("user_agent")){
                            field = "userAgent";
                        }else if (field.equals("session_id")){
                            field = "sessionID";
                        }else if (field.equals("created_at") ||
                                field.equals("craetedAt")){
                            field = "createdAt";
                        }

                        if(m1.getValue().getClass().equals
                                (java.util.LinkedHashMap.class)){
                            LinkedHashMap l = (LinkedHashMap)m1.getValue();
                            Iterator i = l.entrySet().iterator();
                            while(i.hasNext()){
                                Map.Entry m2 = (Map.Entry)i.next();
                                String key = json.get("type")+":"+
                                        m1.getKey().toString() + ":"+m2.getKey();
                                String value = m2.getValue().toString();
                                retVal.put(key,value);
                            }
                        }else{
                            String key = json.get("type") + ":" + field;
                            String value = m1.getValue().toString();
                            retVal.put(key,value);
                        }
                    }

                }
            }catch(ParseException e){
                e.printStackTrace();
            }

            return retVal;
        }
    }

    public static class reducer extends MapReduceBase implements Reducer<Text, IntWritable,Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable>output, Reporter reporter)
                throws IOException{

            int sum = 0;
            while(values.hasNext()){
                sum++;
                values.next();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws IOException{
        Configuration conf = getConf();
        JobConf jobConf = new JobConf(conf, summary.class);
        jobConf.setJobName("summary");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(mapper.class);
        jobConf.setReducerClass(reducer.class);
        jobConf.setCombinerClass(reducer.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(jobConf);
        FileStatus[] statusList = fs.listStatus(new Path(args[1]));
        if(statusList != null){
            for(FileStatus status:statusList){
                FileInputFormat.addInputPath(jobConf,status.getPath());
            }
        }
//        FileInputFormat.setInputPaths(jobConf,new Path(args[1]));
        FileOutputFormat.setOutputPath(jobConf,new Path(args[2]));

        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args)throws Exception{
        int retVal = ToolRunner.run(new Configuration(), new summary(),args);
        System.exit(retVal);
    }
}
