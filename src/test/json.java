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

        LinkedHashMap linkedHashMap = vforce.lei.support.readWithoutPrefix(line);
//        System.out.println(linkedHashMap.get("marker").toString());
//        System.out.println(linkedHashMap.get("itemId").toString());

//        System.out.println(support.getSecond(linkedHashMap.get("createdAt").toString()));

        Iterator it = linkedHashMap.entrySet().iterator();

        System.out.println(("1370882147021").compareTo("1370882207034"));

        List<String> l = new ArrayList<String>();

//        String payloadStr = linkedHashMap.get("popular").toString();
//        String[] payload = payloadStr.substring(1, payloadStr.length()-1).split(",");

//        for(String i:payload){
//            l.add(i);
//        }
//        System.out.println(l.toString());

        List<String> resc = new ArrayList<String>();
        resc.add("fdsaf");
        resc.add("fsdfdsf");
        System.out.println(resc);

        String a = "C";
        char b = a.charAt(0);
        if(b == 'C'){
            System.out.println(b);
            System.out.println("True");
        }

        while(it.hasNext()){
            Map.Entry me = (Map.Entry)it.next();
            System.out.println(me.getKey().toString() + ":" + me.getValue().toString());
        }

    }
}
