package vforce.lei;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class cleaning extends Configured implements Tool{

    public static class mapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text>{

        public static final Log log = LogFactory.getLog(mapper.class);

        public void map(LongWritable key, Text value, OutputCollector<Text, Text>output,
                        Reporter reporter) throws IOException{

            String line = value.toString();
            LinkedHashMap linkedHashMap = support.readWithoutPrefix(line);
            String user = linkedHashMap.get("user").toString();
            Long timeStamp = support.getSecond(linkedHashMap.get("createdAt").toString());
            String session = linkedHashMap.get("sessionID").toString();

            // Prepare the output key of mapper
            String outputKey = user + ","  + session;

            // Prepare the output value of mapper
            String outputValue;
            String type = linkedHashMap.get("type").toString();
            if(type.equals("Account") &&
                    linkedHashMap.get("subAction").toString().equals("parentalControls")){
                outputValue = "x:" + linkedHashMap.get("new").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
            }else if(type.equals("Account")){
                outputValue = "c:"+linkedHashMap.get("subAction").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
            }else if(type.equals("AddToQueue")){
                outputValue = "a:"+linkedHashMap.get("itemId").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
            }else  if(type.equals("Home")){
                outputValue = "P:," + linkedHashMap.get("popular").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
                outputValue = "R:," + linkedHashMap.get("recommended").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
                outputValue = "r:," + linkedHashMap.get("recent").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
            }else  if(type.equals("Hover")){
                outputValue = "h:"+linkedHashMap.get("itemId").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
            }else if(type.equals("ItemPage")){
                outputValue = "i:"+linkedHashMap.get("itemId").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
            }else if(type.equals("Login")){
                outputValue = "L:"+timeStamp;
                output.collect(new Text(outputKey), new Text(outputValue));
            }else  if(type.equals("Logout")){
                outputValue = "l:"+timeStamp;
                output.collect(new Text(outputKey), new Text(outputValue));
            }else if(type.equals("Play") ||
                    type.equals("Pause") ||
                    type.equals("Position") ||
                    type.equals("Stop") ||
                    type.equals("Advance") ||
                    type.equals("Resume")){
                if(linkedHashMap.containsKey("payload") && !linkedHashMap.get("payload").toString().equals("{}")){

                        log.info(linkedHashMap);
                    outputValue = "p:"+linkedHashMap.get("marker").toString() + "," +
                            linkedHashMap.get("itemId").toString()+ ":"+timeStamp;
                        output.collect(new Text(outputKey), new Text(outputValue));

                }
            }else if(type.equals("Queue")){
                outputValue = "q:"+timeStamp;
                output.collect(new Text(outputKey), new Text(outputValue));
            }else if(type.equals("Rate")){
                outputValue = "t:"+linkedHashMap.get("itemId").toString() + "," +
                        linkedHashMap.get("rating").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey), new Text(outputValue));
            }else if(type.equals("Recommendation")){
                outputValue = "C:"+linkedHashMap.get("recs").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey), new Text(outputValue));
            }else if(type.equals("Search")){
                outputValue = "S:"+linkedHashMap.get("results").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey), new Text(outputValue));
            }else  if(type.equals("VerifyPassword")){
                outputValue = "v:"+ timeStamp;
                output.collect(new Text(outputKey), new Text(outputValue));
            }else if(type.equals("WriteReview")){
                outputValue = "w:"+linkedHashMap.get("itemId").toString() + "," +
                        linkedHashMap.get("rating").toString() + "," +
                        linkedHashMap.get("length").toString() + ":"+timeStamp;
                output.collect(new Text(outputKey), new Text(outputValue));

            }

        }
    }

    public static class reducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text>output, Reporter reporter) throws IOException {

            while(values.hasNext()){
                output.collect(key,values.next());
            }
        }
    }

    public int run(String[] args) throws IOException {
        Configuration conf = getConf();
        JobConf jobConf = new JobConf(conf, summary.class);
        jobConf.setJobName("clean");

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setMapperClass(vforce.lei.cleaning.mapper.class);
        jobConf.setReducerClass(vforce.lei.cleaning.reducer.class);

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
        int retVal = ToolRunner.run(new Configuration(), new cleaning(), args);
        System.exit(retVal);
    }
}
