package hdl.lei;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class summaryMR extends Configured implements Tool {

    public static class mapper extends MapReduceBase implements Mapper<LongWritable, Text , Text ,Text> {
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String line = value.toString();

            // Parse each line in the json format and save the result to a LinkedHashMap
            LinkedHashMap result = support.readWithPrefix(line);
            Iterator it = result.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry me = (Map.Entry) it.next();
                System.out.println(me.getKey());
                output.collect(new Text(me.getKey().toString()),
                        new Text(me.getValue().toString()));
            }
        }
    }

    public static class reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {


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
        int retVal = ToolRunner.run(new Configuration(), new summaryMR(), args);
        System.exit(retVal);
    }
}
