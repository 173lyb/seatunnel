package org.apache.seatunnel.connectors.seatunnel.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * @author Kevin Huang
 *     <p>2019年01月18日 16:52:00
 */
public class HttpRequest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpRequest.class);

    private static PoolingHttpClientConnectionManager cm = null;

    static {
        cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(200);
        cm.setDefaultMaxPerRoute(20);
    }

    private static CloseableHttpClient getHttpClient() {
        try {
            SSLContext sslContext =
                    SSLContexts.custom()
                            .loadTrustMaterial(
                                    null,
                                    new TrustStrategy() {
                                        @Override
                                        public boolean isTrusted(
                                                X509Certificate[] x509Certificates, String s)
                                                throws CertificateException {
                                            return true;
                                        }
                                    })
                            .build();
            HttpClientBuilder httpClientBuilder =
                    HttpClients.custom()
                            .setSSLContext(sslContext)
                            .setSSLHostnameVerifier(new NoopHostnameVerifier());
            return httpClientBuilder.build();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (KeyStoreException e) {
            throw new RuntimeException(e);
        } catch (KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

    /** Post请求 */
    public static String post(String url, String data) {
        String result = null;
        // 创建默认的httpClient实例
        CloseableHttpClient httpClient = HttpRequest.getHttpClient();
        CloseableHttpResponse httpResponse = null;
        try {
            HttpPost post = new HttpPost(url);
            // post.addHeader("Content-Type", "application/json");
            Charset charset = StandardCharsets.UTF_8;
            StringEntity entity = new StringEntity(data, charset);
            post.setEntity(entity);
            httpResponse = httpClient.execute(post);
            // response实体
            HttpEntity httpEntity = httpResponse.getEntity();
            if (null != httpEntity) {
                String response = EntityUtils.toString(httpEntity);
                int statusCode = httpResponse.getStatusLine().getStatusCode();
                LOGGER.info("当前请求返回码: {}", statusCode);
                if (statusCode == HttpStatus.SC_OK) {
                    // 成功
                    result = response;
                }
            }
        } catch (IOException e) {
            LOGGER.info("请求失败", e);
        } finally {
            if (httpResponse != null) {
                try {
                    EntityUtils.consume(httpResponse.getEntity());
                    httpResponse.close();
                } catch (IOException e) {
                    LOGGER.info("关闭response失败", e);
                }
            }
        }
        return result;
    }
}
