package hdl.lei;

import com.csvreader.CsvWriter;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;

public class SummaryJava {

    public static void main(String[] args) throws IOException {
        String folder = "data/Jeckle/";
        String fileName = "data/weblog";
        String output = "output/test.csv";

        /*
        File dir = new File(folder);
        File[] files = dir.listFiles();

        for (File file : files) {
            System.out.println(file.getName());
        }
        */

        File out = new File(output);
        if (!out.exists()) {
            out.createNewFile();
        } else {
            out.delete();
            out.createNewFile();
        }

        BufferedReader br = new BufferedReader(new FileReader(fileName));
        CsvWriter csvWriter = new CsvWriter(new FileWriter(output, true), '\t');

        String line;
        Set allKeys = new HashSet();

        // Get all the fields and write to the csv as header
        while ((line = br.readLine()) != null) {
            LinkedHashMap linkedHashMap = read(line);
            Set keys = linkedHashMap.keySet();
            allKeys.addAll(keys);
        }

        if (br != null) {
            br.close();
        }

        Iterator i = allKeys.iterator();

        while (i.hasNext()) {
            csvWriter.write(i.next().toString());
        }
        csvWriter.endRecord();

        br = new BufferedReader(new FileReader(fileName));

        while ((line = br.readLine()) != null) {
            // Find out the largest size list of the LinkedHashMap's values
            LinkedHashMap lm =read(line);
            Iterator it = lm.entrySet().iterator();
            int maxSize=0;
            while(it.hasNext()) {
                Map.Entry me = (Map.Entry) it.next();
                LinkedList lk = (LinkedList) me.getValue();
                int size = lk.size();
                maxSize = (maxSize < size) ? size : maxSize;
            }

            for(int count = 0; count<maxSize;count++){
                Iterator keyIterator = allKeys.iterator();
                while (keyIterator.hasNext()) {
                    String tempKey = keyIterator.next().toString();
                    List lk = (LinkedList)lm.get(tempKey);
                    if (lm.get(tempKey) != null && lk.size() > 0) {
                        if(count >= lk.size() && lk.size() >1 ){
                            csvWriter.write(null);
                        }else{
                            if(lk.size() == 1){
                                csvWriter.write(lk.get(0).toString());
                            }else{
                                if(count >= lk.size()){
                                    csvWriter.write(null);
                                }else{
                                    csvWriter.write(lk.get(count).toString());
                                }
                            }
                        }
                    } else {
                        csvWriter.write(null);
                    }
                }
                csvWriter.endRecord();
            }
        }

        if (csvWriter != null) {
            csvWriter.close();
        }
    }

    public static LinkedHashMap read(String line) {
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
                Map.Entry m1 = (Map.Entry) it.next();
                String field = m1.getKey().toString();

                if (field.equals("type")) {
                    List<String> tmp = new LinkedList<String>();
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
                                List<String> tmp = new LinkedList<String>();
                                tmp.add(m2.getValue().toString());
                                retVal.put(subField,tmp);
                            }
                        }

                    } else {
                        List<String> tmp = new LinkedList<String>();
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

        return retVal;
    }
}
