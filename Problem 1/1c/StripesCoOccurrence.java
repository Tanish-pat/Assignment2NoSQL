import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
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

public class StripesCoOccurrence {
    public static class StripesMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Text word = new Text();
        private Set<String> top50Words = new HashSet<>();
        private int d;

        @Override
        public void setup(Context context) throws IOException {
            d = context.getConfiguration().getInt("distance.d", 1);
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
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");
            for (int i = 0; i < tokens.length; i++) {
                String w1 = tokens[i];
                if (!top50Words.contains(w1)) continue;

                MapWritable stripe = new MapWritable();
                for (int j = Math.max(0, i - d); j <= Math.min(tokens.length - 1, i + d); j++) {
                    if (i == j) continue;
                    String w2 = tokens[j];
                    if (!top50Words.contains(w2)) continue;

                    Text neighbor = new Text(w2);
                    if (stripe.containsKey(neighbor)) {
                        IntWritable count = (IntWritable) stripe.get(neighbor);
                        count.set(count.get() + 1);
                    } else {
                        stripe.put(neighbor, new IntWritable(1));
                    }
                }
                if (!stripe.isEmpty()) {
                    word.set(w1);
                    context.write(word, stripe);
                }
            }
        }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable finalMap = new MapWritable();
            for (MapWritable map : values) {
                for (Map.Entry<Writable, Writable> entry : map.entrySet()) {
                    Text neighbor = (Text) entry.getKey();
                    IntWritable count = (IntWritable) entry.getValue();
                    if (finalMap.containsKey(neighbor)) {
                        IntWritable currentSum = (IntWritable) finalMap.get(neighbor);
                        currentSum.set(currentSum.get() + count.get());
                    } else {
                        finalMap.put(new Text(neighbor), new IntWritable(count.get()));
                    }
                }
            }
            StringBuilder sb = new StringBuilder("{");
            for (Map.Entry<Writable, Writable> entry : finalMap.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
            }
            if (sb.length() > 1) sb.setLength(sb.length() - 2);
            sb.append("}");
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "Stripes Co-Occurrence");
        job.setJarByClass(StripesCoOccurrence.class);
        job.setMapperClass(StripesMapper.class);
        job.setReducerClass(StripesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path("top50.txt").toUri());
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}