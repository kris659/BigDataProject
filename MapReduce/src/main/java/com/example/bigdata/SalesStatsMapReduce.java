package com.example.bigdata;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Locale;

public class SalesStatsMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SalesStatsMapReduce(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "SalesStats");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AvgSizeStationMapper.class);
        job.setCombinerClass(AvgSizeStationCombiner.class);
        job.setReducerClass(AvgSizeStationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesStats.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AvgSizeStationMapper extends Mapper<LongWritable, Text, Text, SalesStats> {

        private final Text key = new Text();

        @Override
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

            if (offset.get() == 0)
                return;

            String[] fields = lineText.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            if(fields.length < 6){
                System.err.println("Invalid input (not enough fields): " + lineText);
                return;
            }

            String productId = fields[1].trim();
            String date = fields[5].trim();

            if(date.length() < 4 || !date.substring(0, 4).matches("\\d{4}")){
                System.err.println("Invalid date format" + lineText);
                return;
            }

            String year = date.substring(0, 4);

            boolean isMobile = fields[4].trim().equalsIgnoreCase("Mobile");

            int quantity;
            double price;

            try{
                quantity = Integer.parseInt(fields[2].trim());
            }
            catch(NumberFormatException e){
                System.err.println("Invalid quantity format: " + lineText + "\n" + e.getMessage());
                return;
            }

            try{
                price = Double.parseDouble(fields[3].trim());
            }
            catch(NumberFormatException e){
                System.err.println("Invalid price format: " + lineText + "\n" + e.getMessage());
                return;
            }

            if (quantity <= 0 || price < 0 || Double.isNaN(price) || Double.isInfinite(price)) {
                return;
            }

            key.set(productId + "," + year);
            context.write(key, new SalesStats(quantity, price * quantity, isMobile ? quantity : 0));
        }
    }

    public static class AvgSizeStationReducer extends Reducer<Text, SalesStats, Text, Text>
    {
        private final Text resultText = new Text();
        SalesStats combinedStats = new SalesStats();

        @Override
        public void reduce(Text key, Iterable<SalesStats> values, Context context) throws IOException, InterruptedException
        {
            combinedStats.reset();

            for (SalesStats val : values) {
                combinedStats.addStats(val);
            }

            int quantity = combinedStats.totalQuantity;
            double average = quantity == 0 ? 0.0 : combinedStats.totalPrice / quantity;

            resultText.set(quantity + "," + String.format(Locale.US, "%.3f", average) + "," + combinedStats.mobileQuantity);

            context.write(key, resultText);
        }
    }

    public static class AvgSizeStationCombiner extends Reducer<Text, SalesStats, Text, SalesStats>
    {
        private final SalesStats combinedStats = new SalesStats();

        @Override
        public void reduce(Text key, Iterable<SalesStats> values, Context context) throws IOException, InterruptedException {

            combinedStats.reset();

            for (SalesStats val : values) {
                combinedStats.addStats(val);
            }

            context.write(key, combinedStats);
        }
    }
}