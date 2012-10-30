//package edu.cmu.ml.rtw.users.matt.randomwalks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.mahout.common.IntPairWritable;

public class WalkFileInputFormat extends FileInputFormat<IntWritable, IntPairWritable> {
    private final static Logger log = Logger.getLogger(WalkFileInputFormat.class);

    @Override
    protected boolean isSplitable(FileSystem file_system, Path filename) {
        return false;
    }

    @Override
    public RecordReader<IntWritable, IntPairWritable> getRecordReader(InputSplit split,
            JobConf conf, Reporter reporter) throws IOException {
        return new WalkFileReader(conf, split);
    }

    public static class WalkFileReader implements RecordReader<IntWritable, IntPairWritable> {
        private Path[] path_list;
        private int current_path = -1;
        private FSDataInputStream current_file = null;
        private FileSystem file_system = null;
        private long total_file_size = 0;
        private long bytes_read = 0;

        public WalkFileReader(Configuration conf, InputSplit split) throws IOException {
            if (!(split instanceof FileSplit) && !(split instanceof MultiFileSplit)) {
                throw new IOException ("InputSplit must be a full file or multiple files");
            }
            if (split instanceof FileSplit) {
                path_list = new Path[1];
                path_list[0] = ((FileSplit) split).getPath();
            } else if (split instanceof MultiFileSplit) {
                path_list = ((MultiFileSplit) split).getPaths();
            }
            file_system = FileSystem.get(conf);

            for (int i=0; i<path_list.length; i++) {
                total_file_size += file_system.getFileStatus(path_list[i]).getLen();
            }

            nextFile();
        }

        @Override
        public boolean next(IntWritable key, IntPairWritable value) {
            if (current_file == null) {
                return false;
            }
            if (!readRecord(key, value)) {
                if (nextFile()) {
                    if (!readRecord(key, value)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            bytes_read += 10; // 4 bytes for an int, 2 bytes for a short, and 4 bytes for an int
            return true;
        }

        private boolean readRecord(IntWritable key, IntPairWritable value) {
            try {
                int walk_id = current_file.readInt();
                int hop = current_file.readShort();
                int vertex = current_file.readInt();
                key.set(walk_id);
                // Hop needs to be first here, because we're going to sort these keys, later, and
                // we want them sorted by hop
                value.set(hop, vertex);
                return true;
            } catch (IOException e) {
                return false;
            }
        }

        private boolean nextFile() {
            try {
                if (current_file != null) {
                    current_file.close();
                }
            } catch (IOException e) {
                current_file = null;
                return false;
            }
            current_file = null;
            current_path += 1;
            if (current_path >= path_list.length) {
                return false;
            }
            try {
                current_file = file_system.open(path_list[current_path]);
            } catch (IOException e) {
                current_file = null;
                return false;
            }
            return true;
        }

        @Override
        public void close() {
            if (current_file == null) {
                return;
            }
            try {
                current_file.close();
                current_file = null;

            } catch (IOException e) {
                current_file = null;
            }
        }

        @Override
        public IntWritable createKey() {
            return new IntWritable();
        }

        @Override
        public IntPairWritable createValue() {
            return new IntPairWritable();
        }

        @Override
        public long getPos() {
            return bytes_read;
        }

        @Override
        public float getProgress() {
            if (total_file_size == 0) {
                return 0.0f;
            }
            return bytes_read / (float) total_file_size;
        }
    }
}
