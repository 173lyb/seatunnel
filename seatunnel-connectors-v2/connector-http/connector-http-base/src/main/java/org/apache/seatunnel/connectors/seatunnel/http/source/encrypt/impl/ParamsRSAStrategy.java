package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.EncryptUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.ACCOUNT;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.DATA;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.PRIVATE_KEY;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.PUBLIC_KEY;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.RSA_DATA;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.SIGN;

public class ParamsRSAStrategy implements EncryptStrategy {
    @Override
    public void encryptBody(
            String body,
            Map<String, Object> bodyMap,
            Map<String, String> bodyEncrypt,
            Map<String, String> params)
            throws SeaTunnelException {}

    @Override
    public Map<String, String> encryptHeader(
            Map<String, String> headers, Map<String, String> headerEncrypt, String body)
            throws SeaTunnelException {
        return headers;
    }

    @Override
    public Map<String, String> encryptParam(
            Map<String, String> params, Map<String, String> paramsEncrypt)
            throws SeaTunnelException {
        Map<String, String> encryptParams = JsonUtils.toMap(JsonUtils.toJsonString(params));
        String account = params.get(ACCOUNT).toString();
        String RSAData = paramsEncrypt.get(RSA_DATA).toString();
        String privateKey = paramsEncrypt.get(PRIVATE_KEY).toString();
        String publicKey = paramsEncrypt.get(PUBLIC_KEY).toString();

        if (StringUtils.isBlank(account)
                || StringUtils.isBlank(RSAData)
                || StringUtils.isBlank(privateKey)
                || StringUtils.isBlank(publicKey)) {
            throw new SeaTunnelException("account, RSAData, privateKey, publicKey 不能为空");
        }
        String RSAData2 = EncryptUtil.encryptByPublicKey(RSAData, publicKey);
        String sign = EncryptUtil.signWithMD5(ACCOUNT + account + DATA + RSAData2);
        encryptParams.put(SIGN, sign);
        encryptParams.put(DATA, RSAData2);
        return encryptParams;
    }
}
