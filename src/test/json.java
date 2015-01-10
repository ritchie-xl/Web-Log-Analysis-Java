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
    public static void test(String[] args) throws IOException{
        String path = "data/test.txt";

        BufferedReader reader = new BufferedReader(new FileReader(path));

        String line = reader.readLine();

        LinkedHashMap linkedHashMap = vforce.lei.support.readWithoutPrefix(line);

        Iterator it = linkedHashMap.entrySet().iterator();

//        List<String> l = new ArrayList<String>();
//
//        List<String> resc = new ArrayList<String>();
//        resc.add("fdsaf");
//        resc.add("fsdfdsf");
//        System.out.println(resc);
//
//        String a = "C";
//        char b = a.charAt(0);
//        if(b == 'C'){
//            System.out.println(b);
//            System.out.println("True");
//        }

//        LinkedHashMap played = (LinkedHashMap)linkedHashMap.get("played");

        if(linkedHashMap.get("type").toString().equals("Account") && linkedHashMap.get("subAction").toString().equals("parentalControls")){

            System.out.println("The flag is X");
        }

        while(it.hasNext()){
            Map.Entry me = (Map.Entry)it.next();
            System.out.println(me.getKey().toString() + ":" + me.getValue().toString());
        }

    }

    public static void main(String[] args) throws IOException{

        String path = "data/test.txt";

        BufferedReader reader = new BufferedReader(new FileReader(path));

        String line = reader.readLine();
        System.out.println(line);

        // Utilize the simple-json to parse all the json file
        JSONParser jsonParser = new JSONParser();

        ContainerFactory containerFactory = new ContainerFactory() {
            @Override
            public Map createObjectContainer() {
                return new LinkedHashMap();
            }

            @Override
            public List creatArrayContainer() {
                return new LinkedList();
            }
        };

        // After parsing the json string, the result will be saved into a map, then
        // the program can traverse the map to get each key: value pair
        try {
            Map json = (Map) jsonParser.parse(line, containerFactory);
            Iterator it = json.entrySet().iterator();

            Set played = ((LinkedHashMap)json.get("played")).keySet();
            System.out.println(played.toString());
            Set rated = ((LinkedHashMap)json.get("rated")).keySet();
            System.out.println(rated.toString());
            Set reviewed = ((LinkedHashMap)json.get("reviewed")).keySet();
            System.out.println(reviewed.toString());

            Set retVal = new TreeSet();
            retVal.addAll(played);
            retVal.addAll(rated);
            retVal.addAll(reviewed);

            System.out.println(retVal.toString());

            String ret = retVal.toString();
            String[] terms = ret.substring(1,ret.length()-1).split(", ");
            Set result = new TreeSet();

            for(String i:terms){
                result.add(i);
            }
            System.out.println(result.toString());

//            while (it.hasNext()) {
//                Map.Entry m1 = (Map.Entry) it.next();
//                String field = m1.getKey().toString();
//
//                // If the key is the type then collect the type as key and "HEAD"
//                // as value in the mapper
//                if (field.equals("type")) {
//                    retVal.put(field, m1.getValue().toString());
//                }else{
//                    // Read all the subfields
//                    if (m1.getValue().getClass().equals
//                            (java.util.LinkedHashMap.class)) {
//                        retVal.put(m1.getKey().toString(),m1.getValue().toString());
//                        LinkedHashMap l = (LinkedHashMap) m1.getValue();
//                        Iterator i = l.entrySet().iterator();
//                        while (i.hasNext()) {
//                            Map.Entry m2 = (Map.Entry) i.next();
//                            String subField = m2.getKey().toString();
//
//                            String key = subField;
//                            String value = m2.getValue().toString();
//                            retVal.put(key, value);
//                        }
//                    } else {
//                        // Handle the values don't have subfields
//                        //                        String key = json.get("type") + ":" + field;
//                        String key = field;
//                        String value = m1.getValue().toString();
//                        retVal.put(key, value);
//                    }
//                }
//            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
