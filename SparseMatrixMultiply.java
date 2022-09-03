import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author liuxinghao
 * @version 1.0 Created on 2014年10月10日
 */
public class SparseMatrixMultiply {
    public static class SMMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag = null;
        private int m = 4;// 矩阵A的行数
        private int p = 2;// 矩阵B的列数

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] val = value.toString().split(",");
            if ("a.txt".equals(flag)) {
                for (int i = 1; i <= p; i++) {
                    context.write(new Text(val[0] + "," + i), new Text("a,"
                            + val[1] + "," + val[2]));
                }
            } else if ("b.txt".equals(flag)) {
                for (int i = 1; i <= m; i++) {
                    context.write(new Text(i + "," + val[1]), new Text("b,"
                            + val[0] + "," + val[2]));
                }
            }
        }
    }

    public static class SMReducer extends
            Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text value : values) {
                String[] val = value.toString().split(",");
                if ("a".equals(val[0])) {
                    mapA.put(val[1], val[2]);
                } else if ("b".equals(val[0])) {
                    mapB.put(val[1], val[2]);
                }
            }

            int result = 0;
            // 可能在mapA中存在在mapB中不存在的key，或相反情况
            // 因为，数据定义的时候使用的是稀疏矩阵的定义
            // 所以，这种只存在于一个map中的key，说明其对应元素为0，不影响结果
            Iterator<String> mKeys = mapA.keySet().iterator();
            while (mKeys.hasNext()) {
                String mkey = mKeys.next();
                if (mapB.get(mkey) == null) {// 因为mkey取的是mapA的key集合，所以只需要判断mapB是否存在即可。
                    continue;
                }
                result += Integer.parseInt(mapA.get(mkey))
                        * Integer.parseInt(mapB.get(mkey));
            }
            context.write(key, new IntWritable(result));
        }
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        String input1 = "D:\\Users\\33708\\Desktop\\SparseMatrix";
        String output = "D:\\Users\\33708\\Desktop\\output";

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "SparseMatrixMultiply");
        job.setJarByClass(SparseMatrixMultiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SMMapper.class);
        job.setReducerClass(SMReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1));// 加载2个输入数据集
        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

