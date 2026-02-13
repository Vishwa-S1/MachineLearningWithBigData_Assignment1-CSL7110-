import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextAnalyzer {

    public static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable unitCount = new IntWritable(1);
        private Text term = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cleanedLine = value.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase();

            StringTokenizer wordSplitter = new StringTokenizer(cleanedLine);
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
            int accumulator = 0;
            for (IntWritable val : values) {
                accumulator += val.get();
            }
            totalCount.set(accumulator);
            context.write(key, totalCount);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job analysisJob = Job.getInstance(config, "text analysis");

        analysisJob.setJarByClass(TextAnalyzer.class);
        analysisJob.setMapperClass(WordMapper.class);
        analysisJob.setReducerClass(SumReducer.class);

        analysisJob.setOutputKeyClass(Text.class);
        analysisJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(analysisJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(analysisJob, new Path(args[1]));

        System.exit(analysisJob.waitForCompletion(true) ? 0 : 1);
    }
}
