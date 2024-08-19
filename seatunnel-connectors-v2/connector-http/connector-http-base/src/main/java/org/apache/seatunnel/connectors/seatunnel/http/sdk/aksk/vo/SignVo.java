package org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.vo;

import org.springframework.http.HttpHeaders;

import lombok.AllArgsConstructor;
import lombok.Data;

/** @author sangfor */
@Data
@AllArgsConstructor
public class SignVo {
    private HttpHeaders authorityHeader;
}
