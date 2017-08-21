package test;

import java.io.File;

public class SetDicUtil {
    public static void main(String[] args) {
        File f = new File("D:\\study\\ESClientWebClient\\dic");
        File[] fs = f.listFiles();
        StringBuilder b = new StringBuilder();
        for (File i : fs){
            b.append(";ext/"+i.getName());
        }
        System.out.println(b.toString());
    }
}
