import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Objects;

import java.io.IOException;

public class PassengerCounter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PassengerCounter(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "SizeProject");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SizeProjectMapper.class);
        job.setCombinerClass(SizeProjectCombinerReducer.class);
        job.setReducerClass(SizeProjectCombinerReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class SizeProjectMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text month = new Text();
        private final IntWritable size = new IntWritable();

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    int check = 0;
                    String line = lineText.toString();
                    int i = 0;
                    for (String word : line
                            //.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                            .split(",")) {
                        if (i == 1) {
                            month.set(word.substring(0, word.lastIndexOf('-')));
                        }
                        if (i == 7) {
                            month.set(month + "\t" + word);
                        }
                        if (i == 3) {
                            size.set(Integer.parseInt(word));
                            check++;
                        }
                        if (i == 9){
                            if(Objects.equals(word, "2")){
                                check++;
                            }
                        }
                        i++;
                    }

                    if(check==2) {
                        context.write(month, size);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class SizeProjectCombinerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int sum = 0;
        private final IntWritable resultValue = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            resultValue.set(sum);
            context.write(key, resultValue);
        }
    }
}