package bigram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class BigramTopFive {

    public static class Bigram implements Writable {
        private String bigram;
        private int count;

        public Bigram(String bigram, int count) {
            this.bigram = bigram;
            this.count = count;
        }

        public Bigram() {
            bigram = "";
            count = 0;
        }

        public String getBigram() {
            return bigram;
        }

        public int getCount() {
            return count;
        }

        public Text toText() {
            return new Text(bigram + " " + count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count = in.readInt();
            bigram = in.readUTF();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(count);
            out.writeUTF(bigram);
        }
    }

    public static class BigramCountComparator implements Comparator<Bigram> {
        @Override
        public int compare(Bigram a, Bigram b) {
            return a.getCount() - b.getCount();
        }
    }

    public static class MapBigram extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Bigram> {
        private Text word = new Text();

        public void map(LongWritable key, Text value,
                OutputCollector<Text, Bigram> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] toke1 = line.split("\t");
            String[] toke2 = toke1[0].split(" ");
            String wordOne = toke2[0], wordTwo = toke2[1], number = toke1[1];
            Bigram bigram = new Bigram(wordOne + " " + wordTwo,
                    Integer.parseInt(number));
            word.set(wordOne);
            output.collect(word, bigram);
            word.set(wordTwo);
            output.collect(word, bigram);
        }
    }

    public static class ReduceBigram extends MapReduceBase implements
            Reducer<Text, Bigram, Text, Text> {
        public void reduce(Text key, Iterator<Bigram> values,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            Comparator<Bigram> comparator = new BigramCountComparator();
            PriorityQueue<Bigram> queue = new PriorityQueue<Bigram>(5,
                    comparator);

            while (values.hasNext()) {
                Bigram nextRef = values.next();
                Bigram next = new Bigram(nextRef.getBigram(),
                        nextRef.getCount());
                if (queue.size() < 5) {
                    queue.add(next);
                } else if (queue.peek().getCount() < next.getCount()) {
                    queue.poll();
                    queue.add(next);
                }
            }

            Iterator<Bigram> iter = queue.iterator();
            while (iter.hasNext()) {
                Bigram nextRef = iter.next();
                Bigram next = new Bigram(nextRef.getBigram(),
                        nextRef.getCount());
                output.collect(key, next.toText());
            } 
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(BigramTopFive.class);
        conf.setJobName("bigramTopFive");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Bigram.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(MapBigram.class);
        conf.setCombinerClass(ReduceBigram.class);
        conf.setReducerClass(ReduceBigram.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
