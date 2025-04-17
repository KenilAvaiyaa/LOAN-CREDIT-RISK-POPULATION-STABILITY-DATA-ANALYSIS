import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.regex.Pattern;
/**
 * Hadoop MapReduce program to calculate the total loan amount by state.
 */
public class LoanDriver {
    /**
     * Mapper class to extract state and loan amount from the input dataset.
     */
    public static class LoanMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text state = new Text();
        private DoubleWritable loanAmount = new DoubleWritable();
        private static final Pattern STATE_PATTERN = Pattern.compile("^[A-Z]{2}$"); // Valid 2-letter state codes
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Assuming CSV format
            try {
                if (fields.length > 22) { // Checking if column exists
                    String loanState = fields[22].trim(); // Extract state (addr_state)
                    double loanAmountValue = Double.parseDouble(fields[2].trim()); // Extract loan amount (loan_amnt)
                 
                    // Process only if the state is a valid US state abbreviation
                    if (STATE_PATTERN.matcher(loanState).matches()) {
                        state.set(loanState);
                        loanAmount.set(loanAmountValue);
                        context.write(state, loanAmount); // Emit state and loan amount
                    }
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                System.err.println("Skipping invalid line: " + value.toString());
            }
        }
    }

    /**
     * Reducer class to sum up loan amounts for each state.
     */
    public static class LoanReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            
            // Sum up loan amounts for the given state
            for (DoubleWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result); // Emit state and total loan amount
        }
    }

    /**
     * Driver class to configure and run the Hadoop MapReduce job.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Loan Amount by State");
        
        job.setJarByClass(LoanDriver.class);
        job.setMapperClass(LoanMapper.class);
        job.setCombinerClass(LoanReducer.class); // Combiner optimizes performance
        job.setReducerClass(LoanReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

