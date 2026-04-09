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

public class Top50Words {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try {
                    BufferedReader reader = new BufferedReader(new FileReader("stopwords.txt"));
                    String stopWord = null;
                    while ((stopWord = reader.readLine()) != null) {
                        stopWords.add(stopWord.trim().toLowerCase());
                    }
                    reader.close();
                } catch (IOException e) {
                    System.err.println("Exception reading stop words file: " + e.getMessage());
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().toLowerCase().split("[^\\w']+");
            for (String token : tokens) {
                if (!token.isEmpty() && !stopWords.contains(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> top50Map = new TreeMap<Integer, String>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            top50Map.put(sum, key.toString());
            if (top50Map.size() > 50) top50Map.remove(top50Map.firstKey());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer count : top50Map.descendingKeySet()) {
                context.write(new Text(top50Map.get(count)), new IntWritable(count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 50 Words Filter");
        job.setJarByClass(Top50Words.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(1); 
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path("stopwords.txt").toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}