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

        Iterator it = linkedHashMap.entrySet().iterator();

        while(it.hasNext()){
            Map.Entry me = (Map.Entry)it.next();
            System.out.println(me.getKey().toString() + ":" + me.getValue().toString());
        }

//        JSONParser parser = new JSONParser();
//
//        ContainerFactory containerFactory = new ContainerFactory() {
//            public Map createObjectContainer() {
//                return new LinkedHashMap();
//            }
//
//            public List creatArrayContainer() {
//                return new LinkedList();
//            }
//        };
//
//        try{
//            Map json = (Map)parser.parse(line, containerFactory);
//            String user = json.get("user").toString();
//            String session = json.get("session_id").toString();
//            String time = json.get("created_at").toString();
//            Long timeSec = support.getSecond(time);
//            String key = user+","+timeSec+","+session;
//            System.out.println(key);
//
//            Iterator it = json.entrySet().iterator();
//            while(it.hasNext()){
//                Map.Entry me = (Map.Entry)it.next();
//                System.out.println(me.getKey() + " : " + me.getValue());
//            }
//
//        }catch(ParseException e){
//            e.printStackTrace();
//        }


//        LinkedHashMap linkedHashMap = vforce.lei.json.readWithPrefix(line);
//        Iterator it = linkedHashMap.entrySet().iterator();
//
//        String userId = linkedHashMap.get("user").toString();
//        String sessionId = linkedHashMap.get("sessionID").toString();
//        String time = linkedHashMap.get("createdAt").toString();
//
//        String key = userId + "," +time+","+sessionId;
//        System.out.println(key);
//
//        while(it.hasNext()){
//            Map.Entry me = (Map.Entry)it.next();
//            System.out.println(me.getKey().toString()+ " : " + me.getValue().toString());
//        }

    }
}
