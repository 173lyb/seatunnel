package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.Factory;

import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;

public interface EncryptStrategyFactory {
    EncryptStrategy create(String encryptType);
}
