package test;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by ritchie on 12/22/14.
 */
public class json {
    public static void main(String[] args) throws IOException{
        String path = "data/test.txt";

        BufferedReader reader = new BufferedReader(new FileReader(path));

        String line = reader.readLine();

        JSONParser parser = new JSONParser();

        ContainerFactory containerFactory = new ContainerFactory() {
            public Map createObjectContainer() {
                return new LinkedHashMap();
            }

            public List creatArrayContainer() {
                return new LinkedList();
            }
        };

        try{
            Map json = (Map)parser.parse(line, containerFactory);
            Iterator it = json.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry me = (Map.Entry)it.next();
                System.out.println(me.getKey() + " : " + me.getValue());
            }

        }catch(ParseException e){
            e.printStackTrace();
        }

    }
}
