package com.tong;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class JSONUtil {
    /**
     * 解析json串，提取所有评论
     */
    public static List<String> parseTag(String json) {
        List<String> list = new ArrayList<String>();
        try {
            //解析json文本成JSONObject
            JSONObject jo = JSON.parseObject(json);
            if (jo != null) {
                JSONArray arr = jo.getJSONArray("extInfoList");
                if (arr != null && arr.size() > 0) {
                    JSONObject o1 = arr.getJSONObject(0);
                    if (o1 != null) {
                        JSONArray arr2 = o1.getJSONArray("values");
                        if (arr2 != null && arr2.size() > 0) {
                            for (int i = 0; i < arr2.size(); i++) {
                                String tag = arr2.getString(i);
                                list.add(tag);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }
}
