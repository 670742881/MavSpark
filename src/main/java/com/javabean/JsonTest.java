package com.javabean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


 public class JsonTest {
        public static void main(String[] args) {
            String stu = "{\"student\":[{\"name\":\"jack\",\"age\":16,\"gender\":\"male\",\"phone\":\"15532867854\"},{\"name\":\"lisa\",\"age\":18,\"gender\":\"female\",\"phone\":\"15732867854\"}]}";
            //1.将字符串解析成json对像
            JSONObject stuJson = JSONObject.parseObject(stu);
            //2.获取json对应中第1条数据，是一个json数组
            JSONArray stuArr = stuJson.getJSONArray("student");
            //3.获取json数组中第1条数据，是一个json对象
            JSONObject stuJack = stuArr.getJSONObject(0);
            //3-1 获取json对象中key是"name"的value值
            String JackName = stuJack.getString("name");
            //3-2 获取json对象中key是"age"的value值
            int JackAge = stuJack.getIntValue("age");
            System.err.println("Student:"+JackName+"\tage:"+JackAge);

            //4.获取json数组中第2条数据，也是是一个json对象
            JSONObject stuLisa = stuArr.getJSONObject(1);
            //4-1 获取json对象中key是"gender"的value值
            String lisaGender = stuLisa.getString("gender");
            //4-2 获取json对象中key是"age"的value值
            int  lisaAge = stuLisa.getIntValue("age");
            System.err.println("Student:Lish"+"\tgender:"+lisaGender);

            //5.移除stuLisa这个json对象中的“phone”
            stuLisa.remove("phone");

            //6.在stuJson中增加一条数据
            stuJson.put("class","a001");

            //7.在stuArr，增加一个student的信息
            JSONObject BobJson = new JSONObject();
            BobJson.put("name","Bob");
            BobJson.put("height",178);
            stuArr.add(2, BobJson);


            System.out.println(stuJson.toJSONString());
        }
    }

