package com.jd.elasticsearch.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class Test {
    private static final String HTTP_PORT = "9200";
    private static final String IP = "127.0.0.1";
    private static final String SecurityKey = "Authorization";//固定值
    private static final String SecurityUser = "elasticsearch";
    private static final String SecurityPassword = ""; //esm  集群管理-->集群列表-->集群信息获取

    public static void main(String[] args) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
//        HttpGet get = new HttpGet("http://" + IP + ":" + HTTP_PORT + "/twitter/_search");
//        get.addHeader(SecurityKey, basicAuthHeaderValue(SecurityUser, SecurityPassword));
//        get.setHeader("Content-Type", "application/json");
//        CloseableHttpResponse response = client.execute(get);
//
//        System.out.println("响应是：" + response);
//        HttpEntity entity = response.getEntity();
//
//        System.out.println("====================");
//        System.out.println(EntityUtils.toString(entity));

        HttpPost httpPost = new HttpPost("http://" + IP + ":" + HTTP_PORT + "/twitter/_update_by_query");
//        HttpPost httpPost = new HttpPost("http://" + IP + ":" + HTTP_PORT + "/_update_by_query");
        JSONObject json = new JSONObject();
        JSONObject content = new JSONObject();
        StringBuffer sc = new StringBuffer("if (ctx._source.country == '') ctx._source.country = '1'");
        Script script = new Script(sc.toString());
        System.out.println("script为：" + script.toString());
        json.put("script", sc.toString());
        // 构建消息实体
        StringEntity postEntity = new StringEntity(json.toString(), Charset.forName("UTF-8"));
        // 发送Json格式的数据请求
        postEntity.setContentEncoding("UTF-8");
        postEntity.setContentType("application/json");
        httpPost.setEntity(postEntity);
        CloseableHttpResponse postResponse = client.execute(httpPost);
        System.out.println("响应是：" + postResponse);
        HttpEntity postResult = postResponse.getEntity();

        System.out.println("====================");
        System.out.println(EntityUtils.toString(postResult));

//        response.close();
        postResponse.close();
        client.close();

    }

    /**
     * 基础的base64生成
     *
     * @param username 用户名
     * @param passwd   密码
     * @return
     */
    private static String basicAuthHeaderValue(String username, String passwd) {
        CharBuffer chars = CharBuffer.allocate(username.length() + passwd.length() + 1);
        byte[] charBytes = null;
        try {
            chars.put(username).put(':').put(passwd.toCharArray());
            charBytes = toUtf8Bytes(chars.array());

            String basicToken = Base64.getEncoder().encodeToString(charBytes);
            return "Basic " + basicToken;
        } finally {
            Arrays.fill(chars.array(), (char) 0);
            if (charBytes != null) {
                Arrays.fill(charBytes, (byte) 0);
            }
        }
    }

    public static byte[] toUtf8Bytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        byte[] bytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(byteBuffer.array(), (byte) 0);
        return bytes;
    }
}