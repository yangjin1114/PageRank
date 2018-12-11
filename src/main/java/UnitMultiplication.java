import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //1 \t 2,3,5,6...
            //output: 1 ---> 2=1/5,...
            String line = value.toString().trim();
            String[] FromTo = line.split("\t");
            //to be replaced
            if (FromTo.length != 2 || FromTo[1].trim().equals("")) {
                return;
            }

            String outputKey = FromTo[0];
            String[] tos = FromTo[1].split(",");

            for (String to : tos) {
                double prob = 1.0 / tos.length;
                String outputValue = to + "=" + prob;
                context.write(new Text(outputKey), new Text(String.valueOf(outputValue)));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input: 1 \t 1, 2 \t 1,...

            String line = value.toString().trim();
            String[] pr = line.split("\t");
            if (pr.length != 2) {
                return;
            }
            //output: 1 ---> 1, 2 ---> 1,...
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input: 1 ---> 1, 1 ---> 2=1/5,...
            double pr = 0;
            Map<String, Double> map = new HashMap<String, Double>();

            for (Text value : values) {
                String curt = value.toString().trim();
                if (curt.indexOf("=") != -1) {
                    String[] toProb = curt.split("=");
                    map.put(toProb[0], Double.parseDouble(toProb[1]));
                } else {
                    pr = Double.parseDouble(curt);
                }
            }

            for (String outputKey : map.keySet()) {
                double outputValue = pr * map.get(outputKey);
                context.write(new Text(outputKey), new Text(String.valueOf(outputValue)));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
