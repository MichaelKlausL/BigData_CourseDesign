import java.io.*;
import java.util.Random;

public class MatrixGenerate {
    //随机矩阵
    public int row = 11;
    public int col = 11;
    public int[][] matrix1 = new int[row][col];
    public int[][] matrix2 = new int[col][row];


    public void Generate() {
        Random ran = new Random();

        //
        for (int i = 0; i < row;i++) {
            for (int j = 0; j < col; j++) {
                matrix1[i][j] = ran.nextInt(20);
                matrix2[i][j] = ran.nextInt(20);
            }
        }


    }

    public void fileWrite(int[][] m, String dir) throws IOException {
        String dir1 = "D:\\Users\\33708\\Desktop\\newMatrix\\ma.txt";
        String dir2 = "D:\\Users\\33708\\Desktop\\newMatrix\\mb.txt";

        File file = new File(dir);
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
        for (int i = 0; i < row;i++) {
            for(int j = 0; j < col; j++) {
                writer.write(","+m[i][j]);
            }
            //换行
            writer.write("\n");
        }
        //writer.write("the first way to write and read");
        writer.flush();
        writer.close();
    }




}
