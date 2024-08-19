package org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.service.impl;

import org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.constants.Constants;
import org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.qo.SignQo;
import org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.service.SigSigner;
import org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.vo.SignVo;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.HmacAlgorithms;
import org.apache.commons.codec.digest.HmacUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;

import org.springframework.http.HttpHeaders;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 签名器
 *
 * @author sangfor
 * @date 2022/7/25
 */
@Slf4j
public class SigSignerJavaImpl implements SigSigner {

    public final String ak;
    public final String sk;

    /**
     * 构造签名器，通过AK/SK构造的签名器与通过AUTHCODE构造的签名器签名结果一致
     *
     * @param ak Access Key
     * @param sk Secret Key
     */
    public SigSignerJavaImpl(String ak, String sk) {
        this.ak = ak;
        this.sk = sk;
    }

    /**
     * 构造签名器，通过AK/SK构造的签名器与通过AUTHCODE构造的签名器签名结果一致
     *
     * @param authCode 联动码
     */
    public SigSignerJavaImpl(String authCode) throws Exception {
        String[] akSk = decodeAuthCode(authCode);
        this.ak = akSk[0];
        this.sk = akSk[1];
    }

    /**
     * 对外暴露签名函数
     *
     * @param request 待加签请求
     */
    @Override
    public void sign(@NonNull HttpRequestBase request) throws Exception {
        Map<String, List<String>> headerMap = new HashMap<>(request.getAllHeaders().length);
        for (Header header : request.getAllHeaders()) {
            headerMap.put(header.getName(), Collections.singletonList(header.getValue()));
        }
        byte[] bytes;
        if (HttpPost.METHOD_NAME.equals(request.getMethod())) {
            InputStream inputStream = ((HttpPost) request).getEntity().getContent();
            bytes = IOUtils.toByteArray(inputStream);
        } else if (HttpGet.METHOD_NAME.equals(request.getMethod())) {
            bytes = new byte[0];
        } else {
            throw new Exception("not support method");
        }
        HttpHeaders headers = new HttpHeaders();
        headers.putAll(headerMap);
        SignQo qo =
                SignQo.builder()
                        .method(request.getMethod())
                        .uri(request.getURI().getPath())
                        .payload(bytes)
                        .host(request.getURI().getHost())
                        .queryStr(request.getURI().getRawQuery())
                        .headers(headers)
                        .build();
        SignVo signVo = sigSign(qo);
        addSdkHeaders(request, signVo);
    }

    /**
     * 对签名结构体进行签名
     *
     * @param signQo 待加签信息
     */
    public SignVo sigSign(@NonNull SignQo signQo) {
        String signDate = getSignDate(signQo.getHeaders());
        addCommonHeaders(signQo, signDate);
        String canonicalStr = concatCanonicalStr(signQo);
        log.debug("canonical str:{}", canonicalStr);
        String canonicalHashStr = Hex.encodeHexString(DigestUtils.sha256(canonicalStr), false);
        log.debug("canonical hash:{}", canonicalHashStr);
        String stringToSign = String.format(Constants.SIGN_STR, signDate, canonicalHashStr);
        log.debug("stringToSign :{}", stringToSign);
        String signature =
                Hex.encodeHexString(
                        new HmacUtils(HmacAlgorithms.HMAC_SHA_256, this.sk).hmac(stringToSign),
                        false);
        log.debug("signature str:{}", signature);
        String authorizationStr = concatAuthorizationStr(signQo, signature);
        log.debug("authorization str:{}", signature);
        HttpHeaders authHeader = addAuthorityHeaders(signQo.getHeaders(), authorizationStr);
        return new SignVo(authHeader);
    }

    /**
     * 添加公共请求头
     *
     * @param signQo 请求头
     * @param signDate 请求时间
     */
    private void addCommonHeaders(SignQo signQo, String signDate) {
        HttpHeaders headers = signQo.getHeaders();
        Map<String, List<String>> map = headers;
        HashMap<String, List<String>> newMap = new HashMap<>(map);
        if (CollectionUtils.isEmpty(headers.getValuesAsList(Constants.SDK_CONTENT_TYPE))) {
            newMap.put(Constants.SDK_CONTENT_TYPE, Collections.singletonList("application/json"));
        }
        newMap.put(Constants.SDK_HOST, Collections.singletonList(signQo.getHost()));
        newMap.put(Constants.SIGN_DATE, Collections.singletonList(signDate));

        headers.putAll(newMap);
        signQo.setHeaders(headers);
    }

    /**
     * 添加Authority请求头
     *
     * @param headers 请头
     * @param authorizationStr 鉴权信息
     * @return 添加过后的请求头
     */
    private HttpHeaders addAuthorityHeaders(HttpHeaders headers, String authorizationStr) {
        Map<String, List<String>> map = headers;
        HashMap<String, List<String>> newMap = new HashMap<>(map);
        newMap.put(Constants.SIG_HEADER_AUTHORIZATION, Collections.singletonList(authorizationStr));
        headers.putAll(newMap);
        return headers;
    }

    /**
     * 拼接Authorization头
     *
     * @param signQo 签名
     * @param signatureStr 特征值
     * @return 请求头
     */
    private String concatAuthorizationStr(SignQo signQo, String signatureStr) {
        return String.format(
                Constants.AUTH_STR, this.ak, getSignedHeaders(signQo.getHeaders()), signatureStr);
    }

    /**
     * 签名结果增加至请求头
     *
     * @param request 请求
     * @param signVo 签名结果
     * @return 请求头
     */
    private void addSdkHeaders(HttpRequestBase request, SignVo signVo) {
        final HttpHeaders headers = Objects.requireNonNull(signVo.getAuthorityHeader());
        for (Map.Entry<String, List<String>> entry :
                ((Map<String, List<String>>) headers).entrySet()) {
            final String headerName = entry.getKey();
            final List<String> headerValues = entry.getValue();
            for (String headerValue : headerValues) {
                request.setHeader(headerName, headerValue);
            }
        }
    }

    /**
     * 将所有请求头的key拼接
     *
     * @param headers 请求头
     * @return 请求头键值对
     */
    private String getSignedHeaders(HttpHeaders headers) {
        List<String> keys =
                headers.keySet().stream()
                        .sorted(Comparator.comparing(String::toLowerCase))
                        .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(keys)) {
            return String.join(";", keys);
        }
        return StringUtils.EMPTY;
    }

    /**
     * 获取签名时间
     *
     * @param headers 请求头
     * @return 签名时间
     */
    private String getSignDate(HttpHeaders headers) {
        if (headers.containsKey(Constants.SIGN_DATE)
                && headers.get(Constants.SIGN_DATE).isEmpty()) {
            return headers.get(Constants.SIGN_DATE).get(0);
        } else {
            return DateFormatUtils.formatUTC(new Date(), Constants.DATE_FORMAT);
        }
    }

    /**
     * 拼接签名字符串
     *
     * @param signQo 请求
     * @return 签名字符串
     */
    private String concatCanonicalStr(@NonNull SignQo signQo) {
        String uri = getUri(signQo.getUri());
        String headerStr = getHeaderStr(signQo.getHeaders());
        return signQo.getMethod().toUpperCase()
                + "\n"
                + uri
                + "\n"
                + getQueryStr(signQo.getQueryStr())
                + "\n"
                + (StringUtils.isBlank(headerStr) ? "" : headerStr)
                + getSignedHeaders(signQo.getHeaders())
                + "\n"
                + getPayload(signQo.getPayload());
    }

    /**
     * 处理请求参数
     *
     * @param queryStr 请求参数
     * @return 处理后的请求参数
     */
    private String getQueryStr(String queryStr) {
        if (StringUtils.isBlank(queryStr)) {
            return StringUtils.EMPTY;
        }
        String[] params = queryStr.split("&");
        Arrays.sort(params, SigSignerJavaImpl::queryStrComparing);
        for (int i = 0; i < params.length; i++) {
            if (!params[i].contains("=")) {
                params[i] = params[i] + "=";
            }
            params[i] = sigEScape(params[i], Constants.SIG_ESCAPE_QUERY);
        }
        return String.join("&", params);
    }

    /**
     * queryStr比对逻辑
     *
     * @param o1 字符串1
     * @param o2 字符串2
     * @return 比对结果
     */
    private static int queryStrComparing(String o1, String o2) {
        String[] o1Arr = o1.split("=");
        String[] o2Arr = o2.split("=");
        String o1Key = o1Arr[0];
        String o1Value = o1Arr[1];
        String o2Key = o2Arr[0];
        String o2Value = o2Arr[1];
        if (o1Key.compareTo(o2Key) == 0) {
            return o1Value.compareTo(o2Value);
        }
        return o1Key.compareTo(o2Key);
    }

    /**
     * 将uri特殊字符处理和转义
     *
     * @param uri uri
     * @return 处理转义后的uri
     */
    private static String getUri(String uri) {
        uri = sigEScape(uri, Constants.SIG_ESCAPE_URI);
        if (!uri.endsWith("/")) {
            uri += "/";
        }
        return uri;
    }

    /**
     * 拼接请求头字符串
     *
     * @param headers 请求头
     * @return key:value; key:value;
     */
    private String getHeaderStr(HttpHeaders headers) {
        Map<String, List<String>> headerMap = headers;
        StringBuilder sb = new StringBuilder();
        List<String> keys =
                headers.keySet().stream()
                        .sorted(Comparator.comparing(String::toLowerCase))
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(keys)) {
            return "";
        }
        for (String key : keys) {
            List<String> headerList = headerMap.get(key);
            if (CollectionUtils.isNotEmpty(headerList)) {
                sb.append(key).append(":").append(headerList.get(0));
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * 获取payload
     *
     * @param payload payload
     * @return 去除空格，重排序，后的payload hash
     */
    private static String getPayload(byte[] payload) {
        String payloadStr = StringUtils.EMPTY;
        if (payload != null) {
            payloadStr = new String(payload);
        }
        payloadStr = payloadStr.replaceAll(" ", "");
        byte[] byteArr = payloadStr.getBytes();
        // 对字节数组重排序
        Arrays.sort(byteArr);
        return Hex.encodeHexString(DigestUtils.sha256(byteArr), false);
    }

    /**
     * 转义
     *
     * @param srcStr 字符串
     * @param type 类型
     * @return 转义后字符串
     */
    public static String sigEScape(String srcStr, int type) {
        if (StringUtils.isBlank(srcStr)) {
            return "";
        }
        String encodedStr = "";
        try {
            encodedStr = URLEncoder.encode(srcStr, String.valueOf(StandardCharsets.UTF_8));
            encodedStr = encodedStr.replaceAll("%7E", "~");
            if (type == Constants.SIG_ESCAPE_URI) {
                encodedStr = encodedStr.replaceAll("%2F", "/");
            }
            if (type == Constants.SIG_ESCAPE_QUERY) {
                encodedStr = encodedStr.replaceAll("%3D", "=");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return encodedStr;
    }

    /**
     * 联动码解码
     *
     * @param authCode 联动码
     * @return 解码后的 AccessKey/SecretKey
     */
    private static String[] decodeAuthCode(String authCode) throws Exception {
        String builderStr = reverseHex(authCode);
        String[] builders = builderStr.split("\\|", -1);
        if (builders.length != Constants.AUTH_CODE_PARAMS_NUM) {
            throw new Exception("auth code decode error");
        }
        byte[] aesSecret = calculateAesSecret(builders);
        String ak = aesCbcDecrypt(builders[9], aesSecret);
        String sk = aesCbcDecrypt(builders[10], aesSecret);
        return new String[] {ak, sk};
    }

    /**
     * 16进制解码
     *
     * @param authCode 联动码
     * @return 解码后的 解码后的联动码参数字符串
     */
    private static String reverseHex(String authCode) throws DecoderException {
        byte[] bytes = Hex.decodeHex(authCode);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 计算aes加密秘钥
     *
     * @param builders 联动码参数列表
     * @return aes加密秘钥
     */
    private static byte[] calculateAesSecret(String[] builders) throws NoSuchAlgorithmException {
        String buildStr =
                String.format(
                        Constants.AUTH_CODE_PARAMS,
                        builders[0],
                        builders[1],
                        builders[2],
                        builders[3],
                        builders[4],
                        builders[5],
                        builders[6],
                        builders[11]);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(buildStr.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * aes解密
     *
     * @param cipherText 要解密的字符换
     * @param key 秘钥
     * @return 解密后的字符串
     */
    private static String aesCbcDecrypt(String cipherText, byte[] key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        SecretKeySpec keySpec = new SecretKeySpec(key, "AES");
        IvParameterSpec ivSpec = new IvParameterSpec(new byte[16]);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
        byte[] decrypted = cipher.doFinal(Hex.decodeHex(cipherText));
        return new String(decrypted, StandardCharsets.UTF_8).trim();
    }
}
