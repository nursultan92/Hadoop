package kg.nursultan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class CustomMatrixMultiplication {

    public static class CustomMapper extends Mapper<Text, MatrixCellWritable, Text, Text> {
        public void map(Text key, MatrixCellWritable value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = 2; //Integer.parseInt(conf.get("m"));
            int p = 3; //Integer.parseInt(conf.get("p"));
            String line = value.toString();
            //String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (key.toString().equals("A")) {
                for (int k = 0; k < p; k++) {
                    outputKey.set(value.rowIndex + "," + k);
                    outputValue.set("A," + value.columnIndex + "," + value.value);
                    context.write(outputKey, outputValue);
                }
            } else {
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + "," + value.columnIndex);
                    outputValue.set("B," + value.rowIndex + "," + value.value);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class CustomReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("A")) {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }
            int n = 5; //Integer.parseInt(context.getConfiguration().get("n"));
            float result = 0.0f;
            float a_ij;
            float b_jk;
            for (int j = 0; j < n; j++) {
                a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result += a_ij * b_jk;
            }
            if (result != 0.0f) {
                context.write(null, new Text(key.toString() + "," + Float.toString(result)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        /*Configuration conf = new Configuration();
        // A is an m-by-n matrix; B is an n-by-p matrix.
        conf.set("m", "2");
        conf.set("n", "5");
        conf.set("p", "3");

        Job job = new Job(conf, "MatrixMatrixMultiplicationOneStep");
        job.setJarByClass(CustomMatrixMultiplication.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(MatrixInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);*/

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix multiplication");
        job.setMapperClass(CustomMapper.class);
        job.setJarByClass(CustomMatrixMultiplication.class);
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(MatrixInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}