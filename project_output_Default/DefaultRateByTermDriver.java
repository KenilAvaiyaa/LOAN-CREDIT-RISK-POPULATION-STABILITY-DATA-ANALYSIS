import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Hadoop MapReduce program to calculate the loan default rate by term.
 */
public class DefaultRateByTermDriver {

    /**
     * Custom Writable class to hold default count and total count for rate calculation.
     */
    public static class CountWritable implements org.apache.hadoop.io.Writable {
        private long defaultCount;
        private long totalCount;

        // Default constructor is required
        public CountWritable() {}

        public CountWritable(long defaultCount, long totalCount) {
            this.defaultCount = defaultCount;
            this.totalCount = totalCount;
        }

        public void set(long defaultCount, long totalCount) {
            this.defaultCount = defaultCount;
            this.totalCount = totalCount;
        }

        public long getDefaultCount() {
            return defaultCount;
        }

        public long getTotalCount() {
            return totalCount;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(defaultCount);
            out.writeLong(totalCount);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            defaultCount = in.readLong();
            totalCount = in.readLong();
        }
    }

    /**
     * Mapper class to extract the term and loan status from the input dataset.
     */
    public static class DefaultRateMapper extends Mapper<Object, Text, Text, CountWritable> {
        private Text term = new Text();
        private CountWritable count = new CountWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header row (assumes header starts with "Unnamed: 0")
            if (line.startsWith("Unnamed: 0")) {
                return;
            }

            // Split the input line. Adjust the delimiter if your file uses a different character.
            String[] fields = line.split(",");
            // Verify that we have enough columns (we need at least 17 fields)
            if (fields.length > 16) {
                // Based on header:
                // Column index 5: term  (e.g., "36 months", "60 months")
                // Column index 16: loan_status (e.g., "Current", "Default", "Charged Off")
                String loanTerm = fields[5].trim();
                String loanStatus = fields[16].trim();

                // Validate that the loan term is one of our expected values and that loanStatus is not empty
                if (loanTerm.matches("^(36|60) months$") && !loanStatus.isEmpty()) {
                    term.set(loanTerm);
                    // Check if loan status indicates a default condition
                    boolean isDefault = loanStatus.equalsIgnoreCase("Default") ||
                                        loanStatus.equalsIgnoreCase("Charged Off");
                    // Set the count: default count is 1 if default, total count is always 1
                    count.set(isDefault ? 1 : 0, 1);
                    context.write(term, count);
                }
            }
        }
    }

    /**
     * Reducer class to compute the default rate for each loan term.
     */
    public static class DefaultRateReducer extends Reducer<Text, CountWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<CountWritable> values, Context context)
                throws IOException, InterruptedException {
            long totalDefaultCount = 0;
            long totalLoanCount = 0;

            // Sum up the default counts and total loan counts for the given term
            for (CountWritable val : values) {
                totalDefaultCount += val.getDefaultCount();
                totalLoanCount += val.getTotalCount();
            }

            // Calculate default rate as a percentage
            if (totalLoanCount > 0) {
                double defaultRate = ((double) totalDefaultCount / totalLoanCount) * 100;
                result.set(defaultRate);
                context.write(key, result);
            }
        }
    }

    /**
     * Driver method to configure and run the Hadoop MapReduce job.
     * 
     * @param args Command-line arguments: input path and output path.
     */
    public static void main(String[] args) throws Exception {
        // Expecting 2 arguments: input path and output path
        if (args.length != 2) {
            System.err.println("Usage: DefaultRateByTermDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Loan Default Rate by Term");

        job.setJarByClass(DefaultRateByTermDriver.class);
        job.setMapperClass(DefaultRateMapper.class);
        job.setReducerClass(DefaultRateReducer.class);

        // Set output key/value types for mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountWritable.class);
        // Set output key/value types for reducer (final output)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // input file/folder in HDFS
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output folder in HDFS

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

