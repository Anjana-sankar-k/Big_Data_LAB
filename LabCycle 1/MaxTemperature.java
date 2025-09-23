import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempByYearHadoop {

    public static class TempMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private IntWritable yearKey = new IntWritable();
        private DoubleWritable tempValue = new DoubleWritable();
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) return;

            String[] parts = line.split("\\s+");  // date, day, temp
            if (parts.length < 3) return;

            try {
                LocalDate date = LocalDate.parse(parts[0], formatter);
                int year = date.getYear();
                double temp = Double.parseDouble(parts[2]); // temperature is 3rd column
                yearKey.set(year);
                tempValue.set(temp);
                context.write(yearKey, tempValue);
            } catch (DateTimeParseException | NumberFormatException e) {
                // skip bad lines
            }
        }
    }

    public static class TempReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double max = Double.NEGATIVE_INFINITY;
            for (DoubleWritable val : values) {
                max = Math.max(max, val.get());
            }
            result.set(max);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTempByYearHadoop <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Temperature By Year");
        job.setJarByClass(MaxTempByYearHadoop.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
        }
                
