package kg.nursultan;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class CustomMatrixMultiplication extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CustomMatrixMultiplication(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        FileUtils.deleteDirectory(new File(args[1]));

        Configuration conf = getConf();
        conf.set("n", args[2]);
        conf.set("m", args[3]);
        conf.set("p", args[4]);
        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setMapperClass(CustomMapper.class);
        job.setJarByClass(getClass());
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(MatrixInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}