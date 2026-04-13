package Problem1a;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top50Words {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // Using try-with-resources for safe and automatic closing of the reader
                try (BufferedReader reader = new BufferedReader(
                        new FileReader(new Path(cacheFiles[0].getPath()).getName()))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                } catch (IOException e) {
                    System.err.println("Exception reading stop words file: " + e.getMessage());
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Exclude non-stop words while computing the frequency
            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");

            for (String token : tokens) {
                // If the token is empty or is found in the stopwords list, skip it
                if (token.isEmpty() || stopWords.contains(token)) {
                    continue;
                }

                word.set(token);
                context.write(word, one);
            }
        }
    }

    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> top50Map = new TreeMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            top50Map.put(sum, key.toString());

            // Keep only the top 50 most frequently occurring words
            if (top50Map.size() > 50) {
                top50Map.remove(top50Map.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the top 50 words in descending order of frequency
            for (Integer count : top50Map.descendingKeySet()) {
                context.write(new Text(top50Map.get(count)), new IntWritable(count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: Top50Words <in> <out> <stopwords_file>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Top 50 Words");
        job.setJarByClass(Top50Words.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(TopNReducer.class);

        // Force a single reducer to find the global top 50
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Utilize the distributed caching option within Hadoop
        job.addCacheFile(new Path(otherArgs[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}