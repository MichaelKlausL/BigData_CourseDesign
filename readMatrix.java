import java.io.*;

public class readMatrix {

    //存储矩阵
    public static int[][] matrix = new int[1001][1001];
    public static int[][] matrix2 = new int[1001][1001];
    public static int[][] matrix3 = new int[1001][1001];
    public static int rowIndex = 11;
    public static int colIndex = 11;



    public static void readFileByLines2(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {

                //将元素存储进matrix
                String[] strList = tempString.split("\t");
                String[] coordinate = strList[0].split(",");

                int row = Integer.parseInt(coordinate[0]);
                int col = Integer.parseInt(coordinate[1]);

                // 坐标存入 matrix3
                matrix3[row][col] = Integer.parseInt(strList[1]);

                //System.out.println(matrix3[row][col]);
            }

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
    public static void readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {

                //将元素存储进matrix
                String[] strList = tempString.split("\t");
                String[] coordinate = strList[0].split(",");

                int row = Integer.parseInt(coordinate[1]);
                int col = Integer.parseInt(coordinate[2]);

                if (coordinate[0].equals("a")) {
                    matrix[row][col] = Integer.parseInt(strList[1]);
                }else {
                    matrix2[row][col] = Integer.parseInt(strList[1]);
                }



                //System.out.println(matrix[row][col]);
            }

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

    public static void writeFile2() throws IOException {
        String dir1 = "D:\\Users\\33708\\Desktop\\outputMatrix.txt";

        File file = new File(dir1);
//如果文件不存在，创建文件
        try {
            if (!file.exists())
                file.createNewFile();
        }catch (IOException e){
            System.out.println(" IOExcetion ");
        }
//创建FileWriter对象
        FileWriter writer = new FileWriter(file);

//向文件中写入内容
        for (int i = 1; i <= rowIndex; i++) {
            for(int j = 1; j <= colIndex; j++) {
                writer.write("," + matrix3[i][j]);

                //debug
                System.out.println(matrix3[i][j]);
            }
            //换行
            writer.write("\n");
        }
        //writer.write("the first way to write and read");
        writer.flush();
        writer.close();

    }


    public static void writeFile() throws IOException {

        String dir1 = "D:\\Users\\33708\\Desktop\\processedMatrix\\ma.txt";
        String dir2 = "D:\\Users\\33708\\Desktop\\processedMatrix\\mb.txt";

        File file = new File(dir1);
        File file2 = new File(dir2);
//如果文件不存在，创建文件
        try {
            if (!file.exists())
                file.createNewFile();
        }catch (IOException e){
            System.out.println(" IOExcetion ");
        }
//创建FileWriter对象
        FileWriter writer = new FileWriter(file);
        FileWriter writer2 = new FileWriter(file2);
//向文件中写入内容
        for (int i = 1; i <= rowIndex; i++) {
            for(int j = 1; j <= colIndex; j++) {
                writer.write("," + matrix[i][j]);
                writer2.write("," + matrix2[i][j]);

                //debug
                System.out.println(matrix[i][j]);
            }
            //换行
            writer.write("\n");
            writer2.write("\n");
        }
        //writer.write("the first way to write and read");
        writer.flush();
        writer.close();

        writer2.flush();
        writer2.close();
    }










    public static void main(String[] args) throws IOException {
//        readFileByLines("D:\\Users\\33708\\Desktop\\tempMatrix.txt");
//        writeFile();

        readFileByLines2("D:\\Users\\33708\\Desktop\\tempMatrix2.txt");
        writeFile2();
    }
}
