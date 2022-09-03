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
 * @version 1.0 Created on 2014年10月9日
 */
public class MatrixMultiply {
    public static class MatrixMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String flag = null;// 数据集名称
        private int rowNum = 11;   // 矩阵A的行数
        private int colNum = 11;   // 矩阵B的列数
        private int rowIndexA = 1; // 矩阵A，当前在第几行
        private int rowIndexB = 1; // 矩阵B，当前在第几行

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            flag = ((FileSplit) context.getInputSplit()).getPath().getName();// 获取文件名称
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            if ("ma.txt".equals(flag)) {
                for (int i = 1; i <= colNum; i++) {
                    Text k = new Text(rowIndexA + "," + i);

                    //判断文件开头是否为 “ ，”
                    if (tokens[0].equals("")){
                        for (int j = 1; j < tokens.length; j++) {
                            Text v = new Text("a," + (j) + "," + tokens[j]);
                            context.write(k, v);
                        }
                    }
                    else {
                        for (int j = 0; j < tokens.length; j++) {
                            Text v = new Text("a," + (j + 1) + "," + tokens[j]);
                            context.write(k, v);
                        }
                    }

                }
                rowIndexA++;// 每执行一次map方法，矩阵向下移动一行
            } else if ("mb.txt".equals(flag)) {
                for (int i = 1; i <= rowNum; i++) {

                    //判断文件开头是否为 “ ，”
                    if (tokens[0].equals("")){
                        for (int j = 1; j < tokens.length; j++) {
                            Text k = new Text(i + "," + (j));
                            Text v = new Text("b," + rowIndexB + "," + tokens[j]);
                            context.write(k, v);
                        }
                    }
                    else {
                        for (int j = 0; j < tokens.length; j++) {
                            Text k = new Text(i + "," + (j + 1));
                            Text v = new Text("b," + rowIndexB + "," + tokens[j]);
                            context.write(k, v);
                        }
                    }
                }
                rowIndexB++;// 每执行一次map方法，矩阵向下移动一行
            }
        }
    }

    public static class MatrixReducer extends
            Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 分别存储矩阵ma、mb中元素
            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text value : values) {
                // 将矩阵元素按 “，” 提取出来
                String[] val = value.toString().split(",");
                if ("a".equals(val[0])) {
                    mapA.put(val[1], val[2]);
                } else if ("b".equals(val[0])) {
                    mapB.put(val[1], val[2]);
                }
            }

            // 计算结果为 result
            int result = 0;
            Iterator<String> mKeys = mapA.keySet().iterator();
            // 遍历 mapA
            while (mKeys.hasNext()) {
                String mkey = mKeys.next();
                if (mapB.get(mkey) == null) {   // 判断mapB是否存在
                    continue;
                }
                // 计算 result
                result += Integer.parseInt(mapA.get(mkey))
                        * Integer.parseInt(mapB.get(mkey));
            }
            context.write(key, new IntWritable(result));
        }
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        // 本地输入地址·
        String input1 = "D:\\Users\\33708\\Desktop\\processedMatrix";
        String output = "D:\\Users\\33708\\Desktop\\output";

        Configuration conf = new Configuration();

        // 提交作业
        Job job = Job.getInstance(conf, "MatrixMultiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        // 设置io格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输出地址
        FileInputFormat.setInputPaths(job, new Path(input1));

        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 结束任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
