package org.apache.seatunnel.connectors.seatunnel.http;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;

/**
 *
 * @Description 绵阳政务中心sha256WithRsa测试
 *
 */
public class Sha256WithRsa {
    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
/*        Map<String, String> map = new HashMap<String, String>();
        String s = "{\"htbh\":\"1169\",\"zjh\":\"510702197209121253\",\"xzqh\":\"510704\"}";
        String yinhaitesttestyh = SecurityUtil.encrypt(s, "yinhaitesttestyh");
        map.put("rsaUserId","20e982d81c154b64881cb464c1f5804d");
        map.put("timestamp","1557804273");
//        map.put("encryptParam",yinhaitesttestyh);
        map.put("response_type","code");
        map.put("client_id","00I2");
        map.put("redirect_uri","http://192.168.3.16:8133/test/code");
        map.put("scope","oauth_info");
        String s1 = JSON.toJSONString(map);*/
        //加密key测试可以使用  yinhaitesttestyh
        String biz_content="{\"clientName\":\"中心\",\"cert_num_man\":\"21028119801230121X\",\"name_man\":\"王岩松\"," +
                "\"cert_num_woman\":\"210225198312240147\",\"name_woman\":\"杨丹玉\"}";
        //        System.out.println(biz_content);
        //接入系统编号
        String access_key = "MYXT119705";
        //服务编号
        String format = "JSON";
        //版本号
        String version = "";
        //流水号
        String request_id = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 20);
        //请求时间戳
        String timestamp = String.valueOf(System.currentTimeMillis());
        //mock服务
        String mock = "";
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.put("biz_content", biz_content);
        paramMap.put("access_key", access_key);
        paramMap.put("version", version);
        paramMap.put("format", format);
        paramMap.put("request_id", request_id);
        paramMap.put("timestamp", timestamp);
        paramMap.put("version", version);
        paramMap.put("mock", mock);

        String source1 = generateSignSource(paramMap);
        //签名串
        //私钥证书放在应用工程的classpath目录（一般是resources目录）下
        String sign = sign(source1, "privateyc.keystore");
        //原生http调用签名串中的+需要转义为%2B (如果使用httpclient、okhttp等组件不需要)
        sign = sign.replaceAll("[+]", "%2B");
//        System.out.println(sign);
        //服务地址"http://10.6.30.111:8082/yhbcp-engine/rest/getZjLastDatums_sw_3"
        String address = "http://10.6.215.230:8082/yhbcp-engine/rest/sgxpt_jhsr_zz";
        //                http://10.6.30.105/yhbcp-engine/rest/findAllRecheck_bdc_ZC
        //参数
        String source =source1.replaceAll("[+]", "%2B");
        String paramStr = source + "&sign=" + sign;
        OutputStreamWriter out = null;
        BufferedReader in = null;
        try {
            URL url = new URL(address);
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            //post传参
            conn.setDoOutput(true);
            conn.setDoInput(true);
            out = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
            out.write(paramStr);
            out.flush();
            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            StringBuilder resultBuilder = new StringBuilder();
            while ((line = in.readLine()) != null) {
                resultBuilder.append(line);
            }
            //结果输出
            System.out.println(resultBuilder.toString());
//          znAdd begin
//            JSONObject jb = new JSONObject(resultBuilder.toString());
//            String biz_data = jb.getString("biz_data");
//            System.out.println(biz_data);
//            JSONObject jb2 = new JSONObject(biz_data);
//            String data = jb2.getString("data");
//
//            System.out.println(data);
//            String decryptedMsg = SecurityUtil.decrypt(data,key);
//            System.out.println(decryptedMsg);
//          znAdd end
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
            } catch (Exception e) {
            }
        }
    }

    public static String generateSignSource(Map params) {
        Set<String> keySet = params.keySet();
        List<String> keys = new ArrayList<String>();
        for (String key : keySet) {
            if (params.get(key) != null && StringUtils.isNotBlank(params.get(key).toString())) {
                keys.add(key);
            }
        }
        Collections.sort(keys);
        StringBuilder builder = new StringBuilder();
        for (int i = 0, size = keys.size(); i < size; i++) {
            String key = keys.get(i);
            Object value = params.get(key);
            builder.append(key);
            builder.append("=");
            builder.append(value);
            //拼接时，不包括最后一个&字符
            if (i != size - 1) {
                builder.append("&");
            }
        }
        return builder.toString();
    }

    private static InputStream getResourceAsStream(String resource) throws IOException {
        InputStream in = null;
        ClassLoader loader = Sha256WithRsa.class.getClassLoader();
        if (loader != null) {
            in = loader.getResourceAsStream(resource);
        }
        if (in == null) {
            in = ClassLoader.getSystemResourceAsStream(resource);
        }
        if (in == null) {
            throw new IOException("请将密钥文件" + resource + "放到工程classpath目录！");
        }
        return in;
    }

    public static String sign(String source, String keyFile) throws Exception {
        //读取解析私钥（解析完成的PrivateKey对象建议缓存起来）
        InputStream in = getResourceAsStream(keyFile);
        PrivateKey privateKey = null;
        String sign = null;
        try {
            byte[] keyBytes = IOUtils.toByteArray(in);
            byte[] encodedKey = Base64.getDecoder().decode(keyBytes);
            KeySpec keySpec = new PKCS8EncodedKeySpec(encodedKey);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            privateKey = keyFactory.generatePrivate(keySpec);
        } finally {
            IOUtils.closeQuietly(in);
        }
        if (privateKey != null) {
            //签名
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(source.getBytes());
            byte[] signed = signature.sign();
            //取base64，得到签名串
            sign = Base64.getEncoder().encodeToString(signed);
        }
        return sign;
    }
    public static String sign2(String source, String keystore) throws Exception {

        PrivateKey privateKey = null;
        String sign = null;
        // Base64解码私钥数据
        byte[] encodedKey = Base64.getDecoder().decode(keystore);
        KeySpec keySpec = new PKCS8EncodedKeySpec(encodedKey);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        privateKey = keyFactory.generatePrivate(keySpec);

        if (privateKey != null) {
            //签名
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(source.getBytes());
            byte[] signed = signature.sign();
            //取base64，得到签名串
            sign = Base64.getEncoder().encodeToString(signed);
        }
        return sign;
    }
}

