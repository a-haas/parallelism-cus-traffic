

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
        extends Mapper<Object, Text, Text, FloatWritable>{
    
        private Text hourArc = new Text();
        private FloatWritable etatArc = new FloatWritable();
          
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {

            String[] arcs = value.toString().split(",");

            // check for etat / hour-min
            if(arcs.length > 11 && arcs[2] != "" && arcs[11] != "" ) { 
                hourArc.set(arcs[11]);
                etatArc.set(Float.parseFloat(arcs[2]));

                context.write(hourArc, etatArc);
            }
        }
    }  
  
    public static class EtatReducer 
        extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, 
            Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            int i = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                // si l'état est inconnu, alors on ne le prend pas en compte
                if(val.get() > 0)
                    i++;
            }
            result.set(sum / i);
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

        // JOB ETAT / NOM
        Job job = new Job(conf, "etat / nom");
        job.setJarByClass(CusTraffic.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setReducerClass(EtatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
    }
}
