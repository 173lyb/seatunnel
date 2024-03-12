package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.Md5Util;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class BodyDtSignatureMd5Strategy implements EncryptStrategy {
    @Override
    public void encryptBody(String body ,Map<String, Object> bodyMap, Map<String, String> bodyEncrypt,Map<String,String> params) throws SeaTunnelException {
        String appKey = bodyMap.get(APP_KEY).toString();
        String appSecret= bodyMap.get(APP_SECRET).toString();
        long timeMillis = System.currentTimeMillis();
        long ts = (timeMillis / 1000) - 80;
        String signature = Md5Util.md5Lower("appKey=" + appKey + "&appSecret=" + appSecret + "&timestamp=" + ts);
        bodyMap.put(SIGNATURE, signature);
        bodyMap.put(TIMESTAMP, ts);
    }

    @Override
    public void encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {

    }

    @Override
    public void encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {

    }
}
