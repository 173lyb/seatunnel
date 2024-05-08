package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class BodyBase64Strategy implements EncryptStrategy {

    @Override
    public void encryptBody(String body ,Map<String, Object> bodyMap, Map<String, String> bodyEncrypt,Map<String,String> params) throws SeaTunnelException {
        Map<String, Object> base64Map = new HashMap<>();
        String base64Str = Base64.getEncoder().encodeToString(body.getBytes());
        base64Map.put(PARAMS, base64Str);
        //秒级时间戳
        base64Map.put(TIMESTAMP, System.currentTimeMillis()/1000);
        //清空bodyMap,转化为base64Map
        bodyMap.clear();
        bodyMap.putAll(base64Map);
    }

    @Override
    public Map<String, String> encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {
        return headers;
    }

    @Override
    public Map<String, String> encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {
        return params;
    }
}
