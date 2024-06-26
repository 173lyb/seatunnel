package org.apache.seatunnel.connectors.seatunnel.http.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.common.utils.JsonUtils;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.SEMICOLON;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.SIGN;

@Slf4j
public class SignUtil {
    public static String sign(Map<String, Object> params, String secretKey) {
        String signStr = params.keySet().stream()
                .filter(key -> !SIGN.equals(key))
                .sorted()
                .map(params::get)
                .filter(Objects::nonNull)
                .map(value -> {
                    if (value instanceof String) {
                        return (String) value;
                    } else {
                        return JsonUtils.toJsonString(value);
                    }
                })
                .collect(Collectors.joining(SEMICOLON)) + SEMICOLON + secretKey;
        String sign = Md5Util.md5Upper(signStr);
        log.info("sign加密串: {}", sign);
        return sign;
    }
}
