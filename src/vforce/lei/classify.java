package vforce.lei;

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
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.*;

public class classify extends Configured implements Tool{

    public static class mapper extends MapReduceBase
            implements Mapper<LongWritable,Text,Text,Text> {
        public void map(LongWritable key, Text value, OutputCollector output, Reporter reporter)
                throws IOException {

            Text outputKey = new Text();
            Text outputValue = new Text();
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

            // After parsing the json string, the result will be saved into a map, then
            // the program can traverse the map to get each key: value pair
            try {
                Map json = (Map) jsonParser.parse(value.toString(), containerFactory);
                Set played = ((LinkedHashMap) json.get("played")).keySet();
                Set rated = ((LinkedHashMap) json.get("rated")).keySet();
                Set reviewed = ((LinkedHashMap) json.get("reviewed")).keySet();


                Set items = new TreeSet();
                items.addAll(played);
                items.addAll(rated);
                items.addAll(reviewed);

                outputKey.set(json.get("userId").toString());
                outputValue.set(json.get("kid") + ":" + items.toString());

                output.collect(outputKey, outputValue);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class reducer extends MapReduceBase
            implements Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            while(values.hasNext()){

                /*String user = key.toString();
                //String start = keys[1];
                //String end = keys[2];

                String value = values.toString();
                String[] terms = value.substring(1,value.length()-1).split(":");
                boolean kid = Boolean.valueOf(terms[0]);
                String[] i = terms[1].substring(1,terms[1].length()-1).split(",");
                Set items = new TreeSet();
                for(String item: i){
                    items.add(item);
                }*/
                output.collect(key,values.next());
            }
        }
    }

    public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            JobConf jobConf = new JobConf(conf, classify.class);
            jobConf.setJobName("classify");

            jobConf.setMapOutputKeyClass(Text.class);
            jobConf.setMapOutputValueClass(Text.class);
            jobConf.setOutputKeyClass(Text.class);
            jobConf.setOutputValueClass(Text.class);

            jobConf.setMapperClass(vforce.lei.classify.mapper.class);
            jobConf.setReducerClass(vforce.lei.classify.reducer.class);

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
        int retVal = ToolRunner.run(new Configuration(), new classify(), args);
        System.exit(retVal);
    }
}
