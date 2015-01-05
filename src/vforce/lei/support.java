package vforce.lei;

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

public class support {
    /* This function is used to parse the line as one json format record
            Input: string in json format
            Output: a LinkedHashMap including the key:value pair of the json file
         */
    public static LinkedHashMap readWithPrefix(String line) {
        LinkedHashMap retVal = new LinkedHashMap<String, String>();

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
                Map.Entry m1 = (Map.Entry) it.next();
                String field = m1.getKey().toString();

                // If the key is the type then collect the type as key and "HEAD"
                // as value in the mapper
                if (field.equals("type")) {
                    retVal.put(json.get("type"), "HEAD");
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

                    // Handle the values have subfields
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

                            // Collect the subfields' type as key and "HEAD" as value for mapper
                            String head = json.get("type") + ":" + m1.getKey().toString();
                            retVal.put(head, "HEAD");

                            // Collect all the subfields' key and value for mapper
                            String key = json.get("type") + ":" +
                                    m1.getKey().toString() + ":" + subField;
                            String value = m2.getValue().toString();
                            retVal.put(key, value);
                        }
                    } else {
                        // Handle the values don't have subfields
                        String key = json.get("type") + ":" + field;
                        String value = m1.getValue().toString();
                        retVal.put(key, value);
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return retVal;
    }

    public static LinkedHashMap readWithoutPrefix(String line) {
        LinkedHashMap retVal = new LinkedHashMap<String, String>();

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
                Map.Entry m1 = (Map.Entry) it.next();
                String field = m1.getKey().toString();

                // If the key is the type then collect the type as key and "HEAD"
                // as value in the mapper
                if (field.equals("type")) {
                    retVal.put(field, m1.getValue().toString());
                }else{

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
                        retVal.put(m1.getKey().toString(),m1.getValue().toString());
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
                            String value = m2.getValue().toString();
                            retVal.put(key, value);
                        }
                    } else {
                        // Handle the values don't have subfields
    //                        String key = json.get("type") + ":" + field;
                        String key = field;
                        String value = m1.getValue().toString();
                        retVal.put(key, value);
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return retVal;
    }



    public static Long getSecond(String in) {
        String dateString = in.substring(0, 10);
        String timeString = in.substring(11, 19);
        String[] dateArray = dateString.split("-");
        String[] timeArray = timeString.split(":");

        int year = Integer.parseInt(dateArray[0]);
        int month = Integer.parseInt(dateArray[1]);
        int day = Integer.parseInt(dateArray[2]);
        int hour = Integer.parseInt(timeArray[0]);
        int minute = Integer.parseInt(timeArray[1]);
        int second = Integer.parseInt(timeArray[2]);

        Calendar calendar = new GregorianCalendar();
        calendar.set(year, month, day, hour, minute, second);
        return calendar.getTimeInMillis();
//        return calendar;
    }
}
