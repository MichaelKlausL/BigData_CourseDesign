import java.io.IOException;

public class MatrixGenTest {


    public static void main(String[] args) throws IOException {
        MatrixGenerate m = new MatrixGenerate();
        m.Generate();
        m.fileWrite(m.matrix1,"D:\\Users\\33708\\Desktop\\newMatrix\\ma.txt");
        m.fileWrite(m.matrix2,"D:\\Users\\33708\\Desktop\\newMatrix\\mb.txt");

    }
}
