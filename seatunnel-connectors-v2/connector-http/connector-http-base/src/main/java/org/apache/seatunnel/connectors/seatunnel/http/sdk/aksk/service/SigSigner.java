package org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.service;

import org.apache.http.client.methods.HttpRequestBase;

import lombok.NonNull;

/** @author sangfor */
public interface SigSigner {

    /**
     * 本方法用来对签名结构体进行签名
     *
     * @param request 签名参数
     * @throws Exception 当签名出现异常时抛出
     */
    void sign(@NonNull HttpRequestBase request) throws Exception;
}
