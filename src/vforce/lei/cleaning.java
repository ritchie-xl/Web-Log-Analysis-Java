package vforce.lei;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.*;

public class cleaning extends Configured implements Tool{

    public static class mapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text>{

        public static final Log log = LogFactory.getLog(mapper.class);

        public void map(LongWritable key, Text value, OutputCollector<Text, Text>output,
                        Reporter reporter) throws IOException{

            String line = value.toString();
            LinkedHashMap linkedHashMap = support.readWithoutPrefix(line);
            String user="";
            try{
                user = linkedHashMap.get("user").toString();
            }catch (NullPointerException e){
                log.info(linkedHashMap.toString());
            }

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
                outputValue = "P:" + linkedHashMap.get("popular").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
                outputValue = "R:" + linkedHashMap.get("recommended").toString()+ ":"+timeStamp;
                output.collect(new Text(outputKey),new Text(outputValue));
                outputValue = "r:" + linkedHashMap.get("recent").toString()+ ":"+timeStamp;
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
            }else if(type.equals("Recommendations")){
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
        implements Reducer<Text, Text, Text,NullWritable>{

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text,NullWritable>output, Reporter reporter) throws IOException {

            JSONObject jsonObject = new JSONObject();

            LinkedHashMap linkedHashMap = new LinkedHashMap();
            // List for recommendations
            List<String> popular = new ArrayList<String>(); // Popular
            List<String> recommended = new ArrayList<String>(); // recommended
            List<String> searched = new ArrayList<String>(); // search
            List<String> hover = new ArrayList<String>();   // hover
            List<String> queued = new ArrayList<String>();  // queued
            List<String> browsed = new ArrayList<String>(); // browsed
            List<String> recommendations = new ArrayList<String>();    // recommendations
            List<String> recent = new ArrayList<String>();
            LinkedHashMap<String, String> played = new LinkedHashMap<String, String>();
            LinkedHashMap<String, String> rated = new LinkedHashMap<String, String>();
            LinkedHashMap<String, LinkedHashMap<String, String>> reviewed =
                    new LinkedHashMap<String, LinkedHashMap<String, String>>();
            List<String> actions = new ArrayList<String>();

            String[] identifier = key.toString().split(",");
            String userId = identifier[0];
            String session = identifier[1];

            List<String> time = new ArrayList<String>();
            String lastTime="";

            while(values.hasNext()){

                String[] value = values.next().toString().split(":");
                String timestamp = value[value.length-1];
                char flag = value[0].charAt(0);
                lastTime = timestamp;
                time.add(timestamp);

                if (flag == 'C'){
                    String[] payload = value[1].substring(1, value[1].length()-1).split(",");
                    for(String i: payload){
                        recommendations.add(i);
                    }
                }else if(flag == 'L'){
                    actions.add("login");
                }else  if(flag == 'P'){
                    String[] payload = value[1].substring(1, value[1].length()-1).split(",");
                    for(String i:payload){
                        popular.add(i);
                    }
                }else  if(flag == 'R'){
                    String[] payload = value[1].substring(1, value[1].length()-1).split(",");
                    for(String i:payload){
                        recommended.add(i);
                    }
                }else if(flag == 'S'){
                    String[] payload = value[1].substring(1, value[1].length()-1).split(",");
                    for(String i:payload){
                        searched.add(i);

                    }
                }else if(flag == 'a'){
                    String payload = value[1];
                    if(!queued.contains(payload)){
                        queued.add(payload);
                    }
                }else if(flag == 'c'){
                    String payload = value[1];
                    actions.add(payload);
                    linkedHashMap.put("kid",false);
                    jsonObject.put("kid",false);
                }else if(flag == 'h'){
                    String payload = value[1];
                    hover.add(payload);
                }else if(flag == 'i'){
                    String payload = value[1];
                    browsed.add(payload);
                }else if(flag == 'l'){
                    actions.add("logout");
                }else if(flag == 'p'){
                    String[] payload = value[1].split(",");
                    played.put(payload[1],payload[0]);
                }else if(flag == 'q'){
                    actions.add("reviewedQueue");
                }else if(flag == 'r'){
                    String[] payload = value[1].substring(1, value[1].length()-1).split(",");
                    for(String i:payload){
                        recent.add(i);
                    }
                }else if(flag == 't'){
                    String[] payload = value[1].split(",");
                    rated.put(payload[0],payload[1]);
                }else if(flag == 'v'){
                    actions.add("verifiedPassword");
                    linkedHashMap.put("kid",false);
                    jsonObject.put("kid",false);
                }else if(flag =='w'){
                    String[] payload = value[1].split(",");
                    LinkedHashMap<String, String> itemId = new LinkedHashMap<String, String>();
                    itemId.put("rating",payload[1]);
                    itemId.put("length",payload[2]);
                    reviewed.put(payload[0],itemId);
                }
                else if(flag == 'x'){
                    linkedHashMap.put("kid",!(value[1].equals("kid")));
                    linkedHashMap.put("end",timestamp);
                    jsonObject.put("kid",!(value[1].equals("kid")));
                    jsonObject.put("end",timestamp);
                    break;
                }
            }

            Collections.sort(time);

            linkedHashMap.put("start",time.get(0));
            linkedHashMap.put("userId",userId);
            linkedHashMap.put("session",session);
            linkedHashMap.put("end",lastTime);
            linkedHashMap.put("recommendations",recommendations);
            linkedHashMap.put("actions",actions);
            linkedHashMap.put("popular",popular);
            linkedHashMap.put("recommended",recommended);
            linkedHashMap.put("searched",searched);
            linkedHashMap.put("queued",queued);
            linkedHashMap.put("hover",hover);
            linkedHashMap.put("browsed",browsed);
            linkedHashMap.put("played",played);
            linkedHashMap.put("recent",recent);
            linkedHashMap.put("rated",rated);
            linkedHashMap.put("reviewed",reviewed);


            jsonObject.put("start",time.get(0));
            jsonObject.put("userId",userId);
            jsonObject.put("session",session);
            jsonObject.put("end",lastTime);
            jsonObject.put("recommendations",recommendations);
            jsonObject.put("actions",actions);
            jsonObject.put("popular",popular);
            jsonObject.put("recommended",recommended);
            jsonObject.put("searched",searched);
            jsonObject.put("queued",queued);
            jsonObject.put("hover",hover);
            jsonObject.put("browsed",browsed);
            jsonObject.put("played",played);
            jsonObject.put("recent",recent);
            jsonObject.put("rated",rated);
            jsonObject.put("reviewed",reviewed);

//            String outputKey = linkedHashMap.toString();
            String outputKey = jsonObject.toJSONString();
            output.collect(new Text(outputKey), null);
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

        // Set Compression type of the mapreduce job
//        jobConf.setBoolean("mapred.output.compress", true);
//        jobConf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        JobClient.runJob(jobConf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int retVal = ToolRunner.run(new Configuration(), new cleaning(), args);
        System.exit(retVal);
    }
}
