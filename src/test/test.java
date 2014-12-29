package test;

import java.util.HashMap;

/**
 * Created by cloudera on 12/29/14.
 */
public class test {
    public static void main(String[] args){
        HashMap hashMap = new HashMap();
        hashMap.put("item",1);
        hashMap.put("name",1);
        hashMap.put("age",1);

        System.out.println(hashMap.keySet().toString());

    }
}
