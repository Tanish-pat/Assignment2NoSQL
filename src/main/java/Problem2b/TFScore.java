package Problem2b;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import opennlp.tools.stemmer.PorterStemmer;

public class TFScore {

    public static class TFStripesMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Map<String, Integer> top100DF = new HashMap<>();
        private PorterStemmer stemmer;
        private Text docId = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            stemmer = new PorterStemmer();
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader reader = new BufferedReader(
                        new FileReader(new Path(cacheFiles[0].getPath()).getName()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            top100DF.put(parts[0], Integer.parseInt(parts[1]));
                        }
                    }
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            docId.set(fileName);

            String line = value.toString().toLowerCase();
            String[] tokens = line.split("[^\\w']+");

            MapWritable stripe = new MapWritable();

            for (String token : tokens) {
                if (token.isEmpty())
                    continue;
                String stemmed = stemmer.stem(token);

                if (top100DF.containsKey(stemmed)) {
                    Text termText = new Text(stemmed);
                    if (stripe.containsKey(termText)) {
                        IntWritable count = (IntWritable) stripe.get(termText);
                        stripe.put(termText, new IntWritable(count.get() + 1));
                    } else {
                        stripe.put(termText, new IntWritable(1));
                    }
                }
            }
            if (!stripe.isEmpty()) {
                context.write(docId, stripe);
            }
        }
    }

    public static class TFScoreReducer extends Reducer<Text, MapWritable, Text, Text> {
        private Map<String, Integer> top100DF = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader reader = new BufferedReader(
                        new FileReader(new Path(cacheFiles[0].getPath()).getName()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length == 2) {
                            top100DF.put(parts[0], Integer.parseInt(parts[1]));
                        }
                    }
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> combinedStripe = new HashMap<>();

            for (MapWritable val : values) {
                for (Map.Entry<Writable, Writable> entry : val.entrySet()) {
                    String term = entry.getKey().toString();
                    int count = ((IntWritable) entry.getValue()).get();
                    combinedStripe.put(term, combinedStripe.getOrDefault(term, 0) + count);
                }
            }

            for (Map.Entry<String, Integer> entry : combinedStripe.entrySet()) {
                String term = entry.getKey();
                int tf = entry.getValue();
                int df = top100DF.get(term);

                double score = tf * Math.log10(10000.0 / df + 1);

                String outValue = term + "\t" + String.format("%.4f", score);
                context.write(key, new Text(outValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // This parses and removes Hadoop-specific arguments like -libjars
        String[] otherArgs = new org.apache.hadoop.util.GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "TF Score Calculation");
        job.setJarByClass(TFScore.class);
        job.setNumReduceTasks(1);

        job.setMapperClass(TFStripesMapper.class);
        job.setReducerClass(TFScoreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(org.apache.hadoop.io.MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Now we safely use otherArgs instead of args
        job.addCacheFile(new Path(otherArgs[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
