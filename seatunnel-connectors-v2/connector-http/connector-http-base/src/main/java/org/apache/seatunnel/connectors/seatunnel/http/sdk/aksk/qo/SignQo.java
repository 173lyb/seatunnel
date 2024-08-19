package org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.qo;

import org.springframework.http.HttpHeaders;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/** @author sangfor */
@Builder
@Getter
@Setter
@AllArgsConstructor
public class SignQo {

    private String method;

    private String uri;

    private String host;

    private byte[] payload;

    private String queryStr;

    private HttpHeaders headers;

    @Override
    public String toString() {
        return "SignQo{"
                + ", method='"
                + method
                + '\''
                + ", uri='"
                + uri
                + '\''
                + ", host='"
                + host
                + '\''
                + ", payload="
                + (Objects.isNull(payload) ? "" : new String(payload))
                + ", queryStr='"
                + queryStr
                + '\''
                + ", headers="
                + headers
                + '}';
    }
}
