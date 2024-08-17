package org.apache.seatunnel.connectors.seatunnel.kafka.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Properties;

import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.JASS_CONF;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KERBEROS_KEYTAB_PATH;
import static org.apache.seatunnel.connectors.seatunnel.kafka.config.Config.KERBEROS_PRINCIPAL;

@Slf4j
public class ConfUtil {
    public static void buildJaasConf(ReadonlyConfig readonlyConfig, Properties properties) {
        StringBuilder sb = new StringBuilder();
        String kerberos_principal = readonlyConfig.get(KERBEROS_PRINCIPAL);
        String kerberos_keytab_path = readonlyConfig.get(KERBEROS_KEYTAB_PATH);

        // Check if the configuration is present and valid.
        readonlyConfig
                .getOptional(JASS_CONF)
                .ifPresent(
                        jassConf -> {
                            // 处理 "com.sun.security.auth.module.Krb5LoginModule" 键的情况
                            if (jassConf.containsKey(
                                    "com.sun.security.auth.module.Krb5LoginModule")) {
                                // 检查 keyTab 和 principal 是否存在
                                if (kerberos_keytab_path == null || kerberos_principal == null) {
                                    throw new SeaTunnelException(
                                            "Missing required configuration: keyTab and principal must be specified when using com.sun.security.auth.module.Krb5LoginModule.");
                                }
                                // 检查 keytab 文件是否存在
                                File keytabFile = new File(kerberos_keytab_path);
                                if (!keytabFile.exists() || !keytabFile.isFile()) {
                                    throw new SeaTunnelException(
                                            "The specified keyTab file does not exist: "
                                                    + kerberos_keytab_path);
                                }
                                // 将 keyTab 和 principal 添加到 jassConf 中
                                jassConf.put("keyTab", kerberos_keytab_path);
                                jassConf.put("principal", kerberos_principal);
                            }

                            // 首先查找并处理 value 是 "required" 的键值对
                            jassConf.entrySet().stream()
                                    .filter(entry -> "required".equals(entry.getValue()))
                                    .findFirst()
                                    .ifPresent(
                                            entry ->
                                                    sb.append(entry.getKey())
                                                            .append(" ")
                                                            .append(entry.getValue())
                                                            .append(" \n"));

                            // 处理剩余的键值对，忽略已经处理过的 "required"
                            jassConf.entrySet().stream()
                                    .filter(entry -> !"required".equals(entry.getValue()))
                                    .forEach(
                                            entry ->
                                                    sb.append(entry.getKey())
                                                            .append("=\"")
                                                            .append(entry.getValue())
                                                            .append("\" \n"));

                            // 删除最后一个换行符，添加分号结束
                            sb.deleteCharAt(sb.length() - 2).append(";");
                            properties.setProperty("sasl.jaas.config", sb.toString());
                            log.info("sasl.jaas.config: {}", sb);
                        });
    }
}
