package Problem1b;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

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

public class PairsCoOccurrence {

    public static class PairsMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();
        private Set<String> top50Words = new HashSet<>();
        private int d;

        @Override
        protected void setup(Context context) throws IOException {
            // Retrieve the distance d, defaulting to 1 if not provided
            d = context.getConfiguration().getInt("distance.d", 1);

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // Safely read the cached top 50 words file dynamically
                try (BufferedReader reader = new BufferedReader(
                        new FileReader(new Path(cacheFiles[0].getPath()).getName()))) {

                    String line;
                    while ((line = reader.readLine()) != null) {
                        // Format expected: count \t word OR word \t count depending on 1a output
                        // Assuming word is the first element based on your split logic,
                        // though you may need to adjust index if 1a outputs (count, word)
                        String[] parts = line.split("\\s+");
                        if (parts.length > 0) {
                            top50Words.add(parts[0].trim());
                        }
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");

            for (int i = 0; i < tokens.length; i++) {
                String w1 = tokens[i];

                // Skip if the word is not in the top 50
                if (!top50Words.contains(w1)) {
                    continue;
                }

                // Look ahead for distance d
                for (int j = i + 1; j <= i + d && j < tokens.length; j++) {
                    String w2 = tokens[j];

                    // Skip if the second word isn't in the top 50, or if it's the exact same word
                    if (!top50Words.contains(w2) || w1.equals(w2)) {
                        continue;
                    }

                    // Emit (w1, w2)
                    pair.set(w1 + "," + w2);
                    context.write(pair, one);

                    // Emit (w2, w1) to ensure the matrix is symmetric
                    pair.set(w2 + "," + w1);
                    context.write(pair, one);
                }
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: PairsCoOccurrence <in> <out> <top50_file>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Pairs Co-Occurrence");
        job.setJarByClass(PairsCoOccurrence.class);

        job.setMapperClass(PairsMapper.class);
        job.setCombinerClass(SumReducer.class); // Added Combiner for optimization
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Add the top 50 words file to the distributed cache
        job.addCacheFile(new Path(otherArgs[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}