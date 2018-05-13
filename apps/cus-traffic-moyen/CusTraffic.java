

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CusTraffic {

    public static class TokenizerMapper 
        extends Mapper<Object, Text, Text, IntWritable>{
    
        private Text nomDateArc = new Text();
          
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {

            String[] arcs = value.toString().split(",");

            // check for date / title / debit
            if(arcs.length > 10 && arcs[10] != "" && arcs[1] != "" && arcs[5] != "") { 
                IntWritable debit = new IntWritable(Integer.parseInt(arcs[5]));
                nomDateArc.set(arcs[10] + "|" + arcs[1]);
 
                context.write(nomDateArc, debit);
            }
        }
    }  
  
    public static class IntSumReducer 
        extends Reducer<Text,IntWritable,Text,IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, 
            Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class AverageByDateMapper 
        extends Mapper<Object, Text, Text, FloatWritable>{
    
        private Text nomArc = new Text();

        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {

            String[] debitByDateAndID = value.toString().split("\\t");

            String[] keySplit = debitByDateAndID[0].split("\\|");
            String date = keySplit[0];
            String name = keySplit[1];

            FloatWritable debit = new FloatWritable(Float.parseFloat(debitByDateAndID[1]));
            nomArc.set(name);
 
            context.write(nomArc, debit);
        }
    }

    public static class AverageByDateSumReducer 
        extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, 
            Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            float i = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                i++;
            }
            float r = sum/i;
            result.set(r);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        FileSystem fs = FileSystem.newInstance(conf);

        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        // JOB DEBIT / JOUR / ARC 
        Job job = new Job(conf, "debit / jour / arc");
        job.setJarByClass(CusTraffic.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);

        // JOB DEBIT MOYEN PAR JOUR / ARC
        Job job2 = new Job(conf, "debit moyen journalier / arc");
        job2.setJarByClass(CusTraffic.class);
        job2.setMapperClass(AverageByDateMapper.class);
        job2.setCombinerClass(AverageByDateSumReducer.class);
        job2.setReducerClass(AverageByDateSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/part-r-00000"));
        String outfinal = new String(otherArgs[1]+"-final");
        FileOutputFormat.setOutputPath(job2, new Path(outfinal));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
