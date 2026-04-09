package Problem2a;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import opennlp.tools.stemmer.PorterStemmer;

public class DocumentFrequency {

    public static class DFMapper extends Mapper<Object, Text, Text, Text> {
        private Set<String> stopWords = new HashSet<>();
        private PorterStemmer stemmer;
        private Text word = new Text();
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
                        stopWords.add(line.trim().toLowerCase());
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

            for (String token : tokens) {
                if (token.isEmpty() || stopWords.contains(token))
                    continue;

                String stemmed = stemmer.stem(token);
                if (!stopWords.contains(stemmed)) {
                    word.set(stemmed);
                    context.write(word, docId);
                }
            }
        }
    }

    public static class DFReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueDocs = new HashSet<>();
            for (Text val : values) {
                uniqueDocs.add(val.toString());
            }
            context.write(key, new IntWritable(uniqueDocs.size()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // This parses and removes Hadoop-specific arguments like -libjars
        String[] otherArgs = new org.apache.hadoop.util.GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Document Frequency");
        job.setJarByClass(DocumentFrequency.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Now we safely use otherArgs instead of args
        job.addCacheFile(new Path(otherArgs[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
