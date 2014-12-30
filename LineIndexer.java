import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class LineIndexer {

    public static class LineIndexMapper extends MapReduceBase implements Mapper<LongWritable,Text,OutputCollector,Reporter> {
    
        private final static Text word = new Text();
        private final static Text location = new Text();
        
        public void map(LongWritable key, Text val,
        OutputCollector output, Reporter reporter)
        throws IOException {
        
            FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            location.set(fileName);
            
            String line = val.toString();
            StringTokenizer itr = new StringTokenizer(line.toLowerCase());
            while (itr.hasMoreTokens()) {
                String temp = itr.nextToken();
                if( temp.contains(".com") && temp.contains("@") && !( temp.contains(" ")) && !(temp.contains("<")) && !(temp.contains("'")) &&!(temp.contains("\""))){
                {
                    word.set(temp);
                    System.out.println( word );
                    output.collect(word, location);
                }
            }
            }
        }
    }
    
    public static class LineIndexReducer extends MapReduceBase implements Reducer <Text,Iterator,OutputCollector,Reporter>{
    
        public void reduce(Text key, Iterator values,
        OutputCollector output, Reporter reporter)
        throws IOException {
        
            boolean first = true;
            StringBuilder toReturn = new StringBuilder();
            while (values.hasNext()){
                if (!first)
                toReturn.append(", ");
                first=false;
                toReturn.append(values.next().toString());
            }
            
            output.collect(key, new Text(toReturn.toString()));
        }
    }
    
    public static void main(String[] args) throws Exception{
        JobConf conf = new JobConf(LineIndexer.class);
        
        conf.setJobName("LineIndexer");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(LineIndexMapper.class);
        conf.setCombinerClass(LineIndexReducer.class);
        conf.setReducerClass(LineIndexReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);
    }
}
