package demo2

import com.alibaba.fastjson.{JSON, JSONObject}

object JsonTest {
  def main(args: Array[String]): Unit = {
    val jsonObj = new JSONObject()
    jsonObj.put("id","A01")
    jsonObj.put("name","Leo")
    jsonObj.put("age",18)
    jsonObj.put("gender","male")
    jsonObj.put("xx",12)

    System.err.println(jsonObj.toString)
    jsonObj.remove("xx")
    System.err.println(jsonObj.toString)

    val people = "[{'name':'Jack','age':23,\"job\":\"worker\"},{'name':'Mary','age':27,\"job\":\"teacher\"}]"

    val jsonArr = JSON.parseArray(people)

    val a = jsonArr.getJSONObject(1)
    System.err.println(s"${a.getIntValue("age")}")
    System.err.println(s"${a.getString("job")}")


  }

}
