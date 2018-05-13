

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
    
        private Text nomArc = new Text();
        private IntWritable etatArc = new IntWritable();
          
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {

            String[] arcs = value.toString().split(",");

            // check for title / etat
            if(arcs.length > 2 && arcs[2] != "" && arcs[1] != "" ) { 
                nomArc.set(arcs[1]);
                etatArc.set(Integer.parseInt(arcs[2]));

                context.write(nomArc, etatArc);
            }
        }
    }  
  
    public static class EtatReducer 
        extends Reducer<Text, IntWritable, Text, Text> {
        
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, 
            Context context
        ) throws IOException, InterruptedException {
            int inconnu = 0;
            int fluide = 0;
            int dense = 0;
            int sature = 0;

            for (IntWritable val : values) {
                int etat = val.get(); 
                if(etat == 0)
                    inconnu+=1;
                else if(etat == 1){
                    fluide+=1;
                }
                else if(etat == 2){
                    dense+=1;
                }
                else if(etat == 3){
                    sature += 1;
                }

            }
            result.set("Inconnu : "+inconnu+" / Fluide : "+fluide+" / Dense : "+dense+" / Saturé : "+sature);
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
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(EtatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
    }
}
