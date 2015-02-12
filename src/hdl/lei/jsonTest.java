package hdl.lei;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class jsonTest {

    public static void main(String[] args) throws Exception {
        String line;
        BufferedReader br = new BufferedReader(new FileReader(("data/test.txt")));

        line = br.readLine();
        System.out.println(line);

        LinkedHashMap retVal = new LinkedHashMap<String, LinkedList<String>>();

        // Clean the data with replacing all the "" with "
        String newLine = line.replaceAll("\"\"", "\"");

        // Utilize the simple-json to parse all the json file
        JSONParser jsonParser = new JSONParser();

            /* Utilize the container to save the result
                Need to implement the createObjectContainer and
                createArrayContainer methods
             */

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
            Map json = (Map) jsonParser.parse(newLine, containerFactory);
            Iterator it = json.entrySet().iterator();
            while (it.hasNext()) {
                List<String> tmp = new ArrayList<String>();
                Map.Entry m1 = (Map.Entry) it.next();
                String field = m1.getKey().toString();

                if (field.equals("type")) {
                    tmp.add(m1.getValue().toString());
                    retVal.put(field, tmp);
                } else {
                    // Normalize all the fields
                    if (field.equals("user_agent")) {
                        field = "userAgent";
                    } else if (field.equals("session_id")) {
                        field = "sessionID";
                    } else if (field.equals("created_at") ||
                            field.equals("craetedAt")) {
                        field = "createdAt";
                    }

                    // Read all the subfields
                    if (m1.getValue().getClass().equals
                            (java.util.LinkedHashMap.class)) {
                        LinkedHashMap l = (LinkedHashMap) m1.getValue();
                        Iterator i = l.entrySet().iterator();
                        while (i.hasNext()) {
                            Map.Entry m2 = (Map.Entry) i.next();
                            String subField = m2.getKey().toString();

                            // Normalize the itemId
                            if (subField.equals("item_id")) {
                                subField = "itemId";
                            }

                            String key = subField;
                            if(m2.getValue().getClass().equals(java.util.LinkedList.class)){
                                retVal.put(subField,m2.getValue());
                            }else{
                                tmp.add(m2.getValue().toString());
                                retVal.put(subField,tmp);
                            }
                        }

                    } else {
                        String key = field;
                        String value = m1.getValue().toString();
                        tmp.add(value);
                        retVal.put(key, tmp);
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Iterator i = retVal.entrySet().iterator();

        while(i.hasNext()){
            Map.Entry m = (Map.Entry)i.next();
            System.out.println(m.getKey().toString() + " : " +m.getValue().toString() );
        }

    }
}
