package org.apache.seatunnel.connectors.seatunnel.http;

import org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.service.impl.SigSignerJavaImpl;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import javax.net.ssl.SSLContext;

import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zsh
 * @description 针对表【api_asset_incidents_data_list_gl(查询安全事件列表)】的数据库操作Service实现
 * @createDate 2023-11-14 17:19:43
 */
public class ApiAssetIncidentsDataListGlServiceImpl {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //        Date start_date = sdf.parse("2023-11-01 00:00:00");
        Date start_date = sdf.parse(MyDateUtil.getDateStrByDayOffset2(-30));
        long start_time = start_date.getTime() / 1000;
        long end_time = System.currentTimeMillis() / 1000;

        HttpPost request = new HttpPost("https://10.32.214.150/api/xdr/v1/incidents/list");

        JSONObject json = new JSONObject();
        json.put("startTimestamp", start_time);
        json.put("endTimestamp", end_time);
        json.put("pageSize", 200);
        json.put("page", 1);
        request.setEntity(new StringEntity(JSON.toJSONString(json)));
        request.setHeader("content-type", "application/json");

        SigSignerJavaImpl sigSignerJava =
                new SigSignerJavaImpl(
                        "63323861323964342D613764372D343937352D626132352D6638656464366163313432387C7C7C73616E67666F727C76317C3132372E302E302E317C7C7C7C37384130464532313442353335323939353131333538323341374534314244354443363844433533354436354344323843464133374442343136373531383832394237353537424344333843304543303437353142344135423339373630463632333532394237394637434334423046463332313245373146424330323338447C37423635323544393141343443384137373744413444333141383433433642374137343834384645364136383435433743433931334536313532343738333737373735303034353445433542323835353542334233364233363843453845304139313336344546353437363837363041333945433138323846443244334131397C7C307C");

        // 签名
        sigSignerJava.sign(request);

        // 发送请求
        SSLContext sslContext =
                new SSLContextBuilder()
                        .loadTrustMaterial(
                                null,
                                new TrustStrategy() {
                                    public boolean isTrusted(X509Certificate[] arg0, String arg1) {
                                        return true;
                                    }
                                })
                        .build();
        SSLConnectionSocketFactory sslsf =
                new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
        HttpClient httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
        HttpResponse response = httpClient.execute(request);
        String content = EntityUtils.toString(response.getEntity(), "UTF-8");
        System.out.println(content);
    }
}
