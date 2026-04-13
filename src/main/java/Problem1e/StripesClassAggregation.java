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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StripesClassAggregation {

    public static class StripesClassAggMapper extends Mapper<Object, Text, Text, MapWritable> {

        private Set<String> top50Words = new HashSet<>();
        private int d;
        // Map-Class level aggregation: Stored at the class level
        private HashMap<String, MapWritable> classStripeMap;

        @Override
        protected void setup(Context context) throws IOException {
            d = context.getConfiguration().getInt("distance.d", 1);
            classStripeMap = new HashMap<>();

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
            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");

            for (int i = 0; i < tokens.length; i++) {
                String w1 = tokens[i];
                if (!top50Words.contains(w1))
                    continue;

                // Retrieve the existing stripe or create a new one
                MapWritable stripe = classStripeMap.getOrDefault(w1, new MapWritable());

                for (int j = Math.max(0, i - d); j <= Math.min(tokens.length - 1, i + d); j++) {
                    if (i == j)
                        continue;
                    String w2 = tokens[j];
                    if (!top50Words.contains(w2))
                        continue;

                    Text neighbor = new Text(w2);
                    if (stripe.containsKey(neighbor)) {
                        IntWritable count = (IntWritable) stripe.get(neighbor);
                        count.set(count.get() + 1);
                    } else {
                        stripe.put(neighbor, new IntWritable(1));
                    }
                }

                if (!stripe.isEmpty()) {
                    classStripeMap.put(w1, stripe);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text outKey = new Text();

            // Emit all accumulated stripes at the end of the Map task
            for (Map.Entry<String, MapWritable> entry : classStripeMap.entrySet()) {
                outKey.set(entry.getKey());
                context.write(outKey, entry.getValue());
            }
        }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            MapWritable finalMap = new MapWritable();

            for (MapWritable map : values) {
                for (Map.Entry<Writable, Writable> entry : map.entrySet()) {
                    Text neighbor = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();

                    if (finalMap.containsKey(neighbor)) {
                        IntWritable current = (IntWritable) finalMap.get(neighbor);
                        current.set(current.get() + count.get());
                    } else {
                        finalMap.put(new Text(neighbor), new IntWritable(count.get()));
                    }
                }
            }

            StringBuilder sb = new StringBuilder("{");
            for (Map.Entry<Writable, Writable> entry : finalMap.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
            }
            if (sb.length() > 1)
                sb.setLength(sb.length() - 2);
            sb.append("}");
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Usage: StripesClassAggregation <in> <out> <top50_file>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Stripes Class Aggregation");
        job.setJarByClass(StripesClassAggregation.class);
        job.setMapperClass(StripesClassAggMapper.class);
        job.setReducerClass(StripesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path(otherArgs[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}