package test;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import vforce.lei.support;

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
        line = reader.readLine();

        LinkedHashMap linkedHashMap = vforce.lei.support.readWithoutPrefix(line);

        System.out.println(linkedHashMap.get("payload"));

        if(linkedHashMap.get("payload").toString().equals("{}")){
            System.out.println("payload is empty");
        }
//        System.out.println(linkedHashMap.get("marker").toString());
//        System.out.println(linkedHashMap.get("itemId").toString());

        System.out.println(support.getSecond(linkedHashMap.get("createdAt").toString()));

        Iterator it = linkedHashMap.entrySet().iterator();

        while(it.hasNext()){
            Map.Entry me = (Map.Entry)it.next();
            System.out.println(me.getKey().toString() + ":" + me.getValue().toString());
        }

    }
}
