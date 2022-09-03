import java.io.*;
import java.io.IOException;
import java.util.*;
import java.lang.Math;

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

public class PreProcess {

    //存储矩阵
    public int[][] matrix = new int[1001][1001];
    public int row = 0;
    public int col = 0;


    public void readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                System.out.println("line " + line + ": " + tempString);
                line++;

                //将元素存储进matrix, 行为 line - 2， 列从 0 到 elements.length
                String[] elements = tempString.split(",");
                for (int i = 0; i < elements.length; i++) {
                    matrix[line - 2][i] = Integer.parseInt(elements[i]);
                }

                //确定矩阵列col
                if (col == 0)
                    col = elements.length;
            }
            //确定矩阵行
            row = line - 1;

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public static class preProMappr extends
            Mapper<LongWritable, Text, Text, Text> {
        private String flag = null;// 数据集名称
        private int rowNum = 11;// 矩阵A的行数
        private int colNum = 11;// 矩阵B的列数
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

            // 分离出 矩阵元素
            String[] tokens = value.toString().split(",");
            // <k2,v2> = <<a,row,col>,val>
            if ("ma.txt".equals(flag)) {
                for (int i = 1; i <= colNum; i++) {
                    //判断文件开头是否为 “ ，”
                    if (tokens[0].equals("")) {      // tokens[1] [2] [3] ...
                        Text v = new Text(tokens[i]);

                        int row = rowIndexA;
                        int col = i;
                        int tempR = row;
                        int tempC = col;

                        // 将自己放入 当前坐标 map 中 <<a,row,col>,<self,val>>
                        // = =
                        Text k = new Text("a," + tempR + "," + tempC);
                        Text val = new Text(" " + tokens[i]);
                        context.write(k, val);

                        if (k.toString().equals("a,100,100"))
                            System.out.println(k);

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug


                        // 每次添加相邻位置都会进行边界判断
                        // + =
                        tempR = (tempR % rowNum) + 1;
                        k = new Text("a," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug

                        // - =
                        tempR--;
                        if (tempR <= 0)
                            tempR = rowNum;
                        k = new Text("a," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug

                        // = +
                        tempC = (tempC % colNum) + 1;
                        k = new Text("a," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug

                        // = -
                        tempC--;
                        if (tempC <= 0)
                            tempC = colNum;
                        k = new Text("a," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug

                        // + +
                        tempR = (tempR % rowNum) + 1;
                        tempC = (tempC % colNum) + 1;
                        k = new Text("a," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug

                        // - -
                        tempR--;
                        if (tempR <= 0)
                            tempR = rowNum;
                        tempC--;
                        if (tempC <= 0)
                            tempC = colNum;
                        k = new Text("a," + tempR + "," + tempC);
                        context.write(k, v);

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug

                        tempR = row;
                        tempC = col;

                        // + -
                        tempR = (tempR % rowNum) + 1;;
                        tempC--;
                        if (tempC <= 0)
                            tempC = colNum;
                        k = new Text("a," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug

                        // - +
                        tempR--;
                        if (tempR <= 0)
                            tempR = rowNum;
                        tempC = (tempC % colNum) + 1;

                        k = new Text("a," + tempR + "," + tempC);
                        context.write(k, v);

                        //debug
                        if (k.toString().equals("a,0,0"))
                            System.out.println(k);
                        //debug


                    }

                }
                rowIndexA++;// 每执行一次map方法，矩阵向下移动一行
            } else if ("mb.txt".equals(flag)) {
                for (int i = 1; i <= colNum; i++) {
                    //判断文件开头是否为 “ ，”
                    if (tokens[0].equals("")) {      // tokens[1] [2] [3] ...
                        Text v = new Text(tokens[i]);

                        int row = rowIndexB;
                        int col = i;
                        int tempR = row;
                        int tempC = col;

                        // 将自己放入 当前位置的 map 中
                        // = =
                        Text k = new Text("b," + tempR + "," + tempC);
                        Text val = new Text(" " + tokens[i]);
                        context.write(k, val);

                        // + =
                        tempR = (tempR % rowNum) + 1;
                        k = new Text("b," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        // - =
                        tempR--;
                        if (tempR <= 0)
                            tempR = rowNum;
                        k = new Text("b," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        // = +
                        tempC = (tempC % rowNum) + 1;
                        k = new Text("b," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        // = -
                        tempC--;
                        if (tempC <= 0)
                            tempC = colNum;
                        k = new Text("b," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        // + +
                        tempR = (tempR % rowNum) + 1;
                        tempC = (tempC % rowNum) + 1;
                        k = new Text("b," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        // - -
                        tempR--;
                        if (tempR <= 0)
                            tempR = rowNum;
                        tempC--;
                        if (tempC <= 0)
                            tempC = colNum;
                        k = new Text("b," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        // + -
                        tempR = (tempR % rowNum) + 1;
                        tempC--;
                        if (tempC <= 0)
                            tempC = colNum;
                        k = new Text("b," + tempR + "," + tempC);
                        context.write(k, v);
                        tempR = row;
                        tempC = col;

                        // - +
                        tempR--;
                        if (tempR <= 0)
                            tempR = rowNum;
                        tempC = (tempC % rowNum) + 1;
                        k = new Text("b," + tempR + "," + tempC);
                        context.write(k, v);
                    }
                }
                rowIndexB++;// 每执行一次map方法，矩阵向下移动一行
            }
        }
    }

    public static class preProcessReducer extends
            Reducer<Text, Text, Text, IntWritable> {

        // 预处理阈值 rate = 0.3
        public double rate = 0.3;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            //debug
            if (key.toString().equals("a,1,1"))
                System.out.println(key);



            //debug
            System.out.println(key);

            if (key.toString().equals("a,100,100"))
                System.out.println(key);

            // 寻找中心点 的 值
            String centerStr = "0";
            int center = 0;

            for (Text val: values) {
                String str = val.toString();
                if (str.startsWith(" ")) {
                    centerStr = str;
                    break;
                }
            }
            // 去除centerStr第一个元素
            centerStr = centerStr.substring(1);
            center = Integer.parseInt(centerStr);



            // 剔除偏差最大的两个值

            // 求平均值
            int average = 0;
            for (Text val: values) {
                String str = val.toString();
                int temp = Integer.parseInt(str);
                average += temp;
            }
            average = average / 9;


            // 修改 中心值  (rate = 0.3)
            double tempD = Math.abs(center - average);
            tempD = tempD / Math.max(center, average);
            if (tempD > rate) {
                center = average;
            }

            // 写入 context
            String str = key.toString() + ",";//
            key = new Text(str);              //

            IntWritable v = new IntWritable(center);
            context.write(key, v);
        }

    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 设置本地路径
        String input1 = "D:\\Users\\33708\\Desktop\\newMatrix";
        String output = "D:\\Users\\33708\\Desktop\\output";

        Configuration conf = new Configuration();
        // 提交作业
        Job job = Job.getInstance(conf, "PreProcess");
        job.setJarByClass(PreProcess.class);

        //set Map
        job.setMapperClass(PreProcess.preProMappr.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //set Reduce
        job.setReducerClass(PreProcess.preProcessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path(input1));
        // 设置输出路径
        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}

