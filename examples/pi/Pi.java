import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Pi extends Configured implements Tool
{
    static private final Path TMP_DIR = new Path(Pi.class.getSimpleName()
            + "_TMP_3_141592654");

    public static class PiMapper extends MapReduceBase implements
            Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable>
    {

        public void map(LongWritable offset, LongWritable size,
                OutputCollector<BooleanWritable, LongWritable> out,
                Reporter reporter) throws IOException
        {
            long index = offset.get();

            long k;

            double x0 = 0;
            double[] q0 = new double[63];
            int[] d0 = new int[63];

            q0[0] = 1.0 / 2;
            d0[0] = (int)(index % 2);
            k = (index - d0[0]) / 2;
            x0 += d0[0] * q0[0];
            for(int j = 1; j < 63; ++j)
            {
                q0[j] = q0[j - 1] / 2;
                d0[j] = (int)(k % 2);
                k = (k - d0[j]) / 2;
                x0 += d0[j] * q0[j];
            }

            double x1 = 0;
            double[] q1 = new double[40];
            int[] d1 = new int[40];

            q1[0] = 1.0 / 3;
            d1[0] = (int)(index % 3);
            k = (index - d1[0]) / 3;
            x1 += d1[0] * q1[0];
            for(int j = 1; j < 40; ++j)
            {
                q1[j] = q1[j - 1] / 3;
                d1[j] = (int)(k % 3);
                k = (k - d1[j]) / 3;
                x1 += d1[j] * q1[j];
            }

            long inside = 0;
            long outside = 0;

            for(long i = 0; i < size.get(); ++i)
            {
                ++index;

                for(int j = 0; j < 63; ++j)
                {
                    ++d0[j];
                    x0 += q0[j];
                    if(d0[j] < 2)
                        break;
                    d0[j] = 0;
                    x0 -= (j == 0 ? 1.0 : q0[j - 1]);
                }

                for(int j = 0; j < 40; ++j)
                {
                    ++d1[j];
                    x1 += q1[j];
                    if(d1[j] < 3)
                        break;
                    d1[j] = 0;
                    x1 -= (j == 0 ? 1.0 : q1[j - 1]);
                }

                double x = x0 - .5;
                double y = x1 - .5;

                if(x * x + y * y <= .25)
                    ++inside;
                else
                    ++outside;
            }

            out.collect(new BooleanWritable(true), new LongWritable(inside));
            out.collect(new BooleanWritable(false), new LongWritable(outside));
        }
    }

    public static class PiReducer extends MapReduceBase
            implements
            Reducer<BooleanWritable, LongWritable, WritableComparable<?>, Writable>
    {

        private long numInside = 0;
        private long numOutside = 0;
        private JobConf conf; // configuration for accessing the file system

        @Override
        public void configure(JobConf job)
        {
            conf = job;
        }

        public void reduce(BooleanWritable isInside,
                Iterator<LongWritable> values,
                OutputCollector<WritableComparable<?>, Writable> output,
                Reporter reporter) throws IOException
        {
            if(isInside.get())
            {
                for(; values.hasNext(); numInside += values.next().get())
                    ;
            }
            else
            {
                for(; values.hasNext(); numOutside += values.next().get())
                    ;
            }
        }

        @Override
        public void close() throws IOException
        {
            // write output to a file
            Path outDir = new Path(TMP_DIR, "out");
            Path outFile = new Path(outDir, "reduce-out");
            FileSystem fileSys = FileSystem.get(conf);
            SequenceFile.Writer writer = SequenceFile.createWriter(fileSys,
                    conf, outFile, LongWritable.class, LongWritable.class,
                    CompressionType.NONE);
            writer.append(new LongWritable(numInside), new LongWritable(
                    numOutside));
            writer.close();
        }
    }

    public static BigDecimal estimate(int numMaps, long numPoints,
            JobConf jobConf) throws IOException
    {

        jobConf.setJobName(Pi.class.getSimpleName());
        jobConf.setInputFormat(SequenceFileInputFormat.class);
        jobConf.setOutputKeyClass(BooleanWritable.class);
        jobConf.setOutputValueClass(LongWritable.class);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);
        jobConf.setMapperClass(PiMapper.class);
        jobConf.setNumMapTasks(numMaps);
        jobConf.setReducerClass(PiReducer.class);
        jobConf.setNumReduceTasks(1);
        jobConf.setSpeculativeExecution(false);

        final Path inDir = new Path(TMP_DIR, "in");
        final Path outDir = new Path(TMP_DIR, "out");
        FileInputFormat.setInputPaths(jobConf, inDir);
        FileOutputFormat.setOutputPath(jobConf, outDir);

        final FileSystem fs = FileSystem.get(jobConf);
        if(fs.exists(TMP_DIR))
        {
            throw new IOException("Tmp directory " + fs.makeQualified(TMP_DIR)
                    + " already exists.  Please remove it first.");
        }
        if(!fs.mkdirs(inDir))
        {
            throw new IOException("Cannot create input directory " + inDir);
        }

        try
        {
            for(int i = 0; i < numMaps; ++i)
            {
                final Path file = new Path(inDir, "part" + i);
                final LongWritable offset = new LongWritable(i * numPoints);
                final LongWritable size = new LongWritable(numPoints);
                final SequenceFile.Writer writer = SequenceFile.createWriter(
                        fs, jobConf, file, LongWritable.class,
                        LongWritable.class, CompressionType.NONE);
                try
                {
                    writer.append(offset, size);
                }
                finally
                {
                    writer.close();
                }
                System.out.println("Wrote input for Map #" + i);
            }

            System.out.println("Starting Job");
            final long startTime = System.currentTimeMillis();
            JobClient.runJob(jobConf);
            final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
            System.out.println("Job Finished in " + duration + " seconds");

            Path inFile = new Path(outDir, "reduce-out");
            LongWritable numInside = new LongWritable();
            LongWritable numOutside = new LongWritable();
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile,
                    jobConf);
            try
            {
                reader.next(numInside, numOutside);
            }
            finally
            {
                reader.close();
            }

            return BigDecimal.valueOf(4).setScale(20)
                    .multiply(BigDecimal.valueOf(numInside.get()))
                    .divide(BigDecimal.valueOf(numMaps))
                    .divide(BigDecimal.valueOf(numPoints));
        }
        finally
        {
            fs.delete(TMP_DIR, true);
        }
    }

    public int run(String[] args) throws Exception
    {
        if(args.length != 2)
        {
            System.err.println("Usage: " + getClass().getName()
                    + " <nMaps> <nSamples>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        final int nMaps = Integer.parseInt(args[0]);
        final long nSamples = Long.parseLong(args[1]);

        System.out.println("Number of Maps  = " + nMaps);
        System.out.println("Samples per Map = " + nSamples);

        final JobConf jobConf = new JobConf(getConf(), getClass());
        System.out.println("Estimated value of Pi is "
                + estimate(nMaps, nSamples, jobConf));
        return 0;
    }

    public static void main(String[] argv) throws Exception
    {
        System.exit(ToolRunner.run(null, new Pi(), argv));
    }
}