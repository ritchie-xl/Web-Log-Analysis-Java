package hdl.lei;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.BufferedReader;
import java.io.FileReader;

public class time {
    public static void main(String[] args) throws Exception{
        BufferedReader br = new BufferedReader(new FileReader("output/createdAt.csv"));
        CsvWriter csvWriter = new CsvWriter("output/result.csv");
        
        String line="";
        int sum = 0;
        String tmp;
        int count = 8;
        
        line = br.readLine();
        String[] terms = line.split(",");
        sum = Integer.parseInt(terms[1]);
        tmp = terms[0];
        
        while((line=br.readLine())!=null){
            terms = line.split(",");
            if(terms[0].equals(tmp)){
                sum = sum + Integer.parseInt(terms[1]);
            }else{
                csvWriter.write(String.valueOf(count));
                csvWriter.write(String.valueOf(sum));
                csvWriter.endRecord();
                sum = Integer.parseInt(terms[1]);
                tmp = terms[0];
                count ++;
            }   
            
        }
        csvWriter.close();
        br.close();
    }
}
