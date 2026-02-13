import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable unitCount = new IntWritable(1);
        private Text term = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String processedLine = value.toString().replaceAll("[^a-zA-Z ]", "");

            StringTokenizer wordSplitter = new StringTokenizer(processedLine);
            while (wordSplitter.hasMoreTokens()) {
                term.set(wordSplitter.nextToken());
                context.write(term, unitCount);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalCount = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalCount.set(sum);
            context.write(key, totalCount);
        }
    }

    public static void main(String[] args) throws Exception {
        long beginTime = System.currentTimeMillis();

        Configuration config = new Configuration();
        config.setLong("mapreduce.input.fileinputformat.split.maxsize", 67108864);

        Job analysisJob = Job.getInstance(config, "word count");
        analysisJob.setJarByClass(WordCount.class);
        analysisJob.setMapperClass(TokenizerMapper.class);
        analysisJob.setCombinerClass(SumReducer.class);
        analysisJob.setReducerClass(SumReducer.class);
        analysisJob.setOutputKeyClass(Text.class);
        analysisJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(analysisJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(analysisJob, new Path(args[1]));

        boolean completed = analysisJob.waitForCompletion(true);

        long finishTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (finishTime - beginTime) + " ms");

        System.exit(completed ? 0 : 1);
    }
}
