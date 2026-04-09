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

public class PairsClassAggregation {
    public static class PairsClassAggMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Set<String> top50Words = new HashSet<>();
        private int d;
        private HashMap<String, Integer> localMap;

        @Override
        public void setup(Context context) throws IOException {
            d = context.getConfiguration().getInt("distance.d", 1);
            localMap = new HashMap<>(); 
            
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("top50.txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length > 0) top50Words.add(parts[0].trim());
                }
                reader.close();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) {
            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");
            for (int i = 0; i < tokens.length; i++) {
                String w1 = tokens[i];
                if (!top50Words.contains(w1)) continue;

                for (int j = i + 1; j <= i + d && j < tokens.length; j++) {
                    String w2 = tokens[j];
                    if (!top50Words.contains(w2) || w1.equals(w2)) continue;

                    String pair1 = w1 + "," + w2;
                    String pair2 = w2 + "," + w1;

                    localMap.put(pair1, localMap.getOrDefault(pair1, 0) + 1);
                    localMap.put(pair2, localMap.getOrDefault(pair2, 0) + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
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
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "Pairs Class Aggregation");
        job.setJarByClass(PairsClassAggregation.class);
        job.setMapperClass(PairsClassAggMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path("top50.txt").toUri());
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}