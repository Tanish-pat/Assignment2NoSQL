package Problem1e;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

public class PairsFunctionAggregation {

    public static class PairsFuncAggMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Set<String> top50Words = new HashSet<>();
        private int d;

        @Override
        protected void setup(Context context) throws IOException {
            d = context.getConfiguration().getInt("distance.d", 1);

            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader reader = new BufferedReader(
                        new FileReader(new Path(cacheFiles[0].getPath()).getName()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\\s+");
                        if (parts.length > 0) {
                            top50Words.add(parts[0].trim());
                        }
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Map-Function level aggregation: Map is initialized inside the function call
            HashMap<String, Integer> localMap = new HashMap<>();

            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");

            for (int i = 0; i < tokens.length; i++) {
                String w1 = tokens[i];
                if (!top50Words.contains(w1))
                    continue;

                for (int j = i + 1; j <= i + d && j < tokens.length; j++) {
                    String w2 = tokens[j];
                    if (!top50Words.contains(w2) || w1.equals(w2))
                        continue;

                    String pair1 = w1 + "," + w2;
                    String pair2 = w2 + "," + w1;

                    localMap.put(pair1, localMap.getOrDefault(pair1, 0) + 1);
                    localMap.put(pair2, localMap.getOrDefault(pair2, 0) + 1);
                }
            }

            // Emit at the end of the map function
            Text outKey = new Text();
            IntWritable outVal = new IntWritable();
            for (Map.Entry<String, Integer> entry : localMap.entrySet()) {
                outKey.set(entry.getKey());
                outVal.set(entry.getValue());
                context.write(outKey, outVal);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Usage: PairsFunctionAggregation <in> <out> <top50_file>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Pairs Function Aggregation");
        job.setJarByClass(PairsFunctionAggregation.class);
        job.setMapperClass(PairsFuncAggMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path(otherArgs[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}