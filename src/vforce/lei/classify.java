package vforce.lei;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class classify extends Configured implements Tool{

    public static class mapper extends MapReduceBase implements Mapper {

        @Override
        public void map(Object o, Object o2, OutputCollector outputCollector, Reporter reporter) throws IOException {

        }
    }

    public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            JobConf jobConf = new JobConf(conf, summary.class);
            jobConf.setJobName("classify");

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
        int retVal = ToolRunner.run(new Configuration(), new classify(), args);
        System.exit(retVal);
    }
}
