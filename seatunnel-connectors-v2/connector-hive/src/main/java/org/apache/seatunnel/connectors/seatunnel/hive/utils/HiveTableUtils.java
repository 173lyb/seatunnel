/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConstants;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

public class HiveTableUtils {

    public static Table getTableInfo(ReadonlyConfig readonlyConfig) {
        String table = readonlyConfig.get(HiveSourceOptions.TABLE_NAME);
        TablePath tablePath = TablePath.of(table);
        if (tablePath.getDatabaseName() == null || tablePath.getTableName() == null) {
            throw new SeaTunnelRuntimeException(
                    HiveConnectorErrorCode.HIVE_TABLE_NAME_ERROR, "Current table name is " + table);
        }
        HiveMetaStoreProxy hiveMetaStoreProxy = HiveMetaStoreProxy.getInstance(readonlyConfig);
        try {
            return hiveMetaStoreProxy.getTable(
                    tablePath.getDatabaseName(), tablePath.getTableName());
        } finally {
            hiveMetaStoreProxy.close();
        }
    }

    public static FileFormat parseFileFormat(Table table) {
        String inputFormat = table.getSd().getInputFormat();
        if (HiveConstants.TEXT_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.TEXT;
        }
        if (HiveConstants.PARQUET_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.PARQUET;
        }
        if (HiveConstants.ORC_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.ORC;
        }
        throw new HiveConnectorException(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                "Hive connector only support [text parquet orc] table now");
    }

    /**
     * @param readonlyConfig "hive.s3_hive_site_path" = "/xugurtp/2024/09/lyb/hive-site.xml"
     *     "hive.s3_hdfs_site_path" = "/xugurtp/2024/09/lyb/hdfs-site.xml" "hive.s3_core_site_path"
     *     = "/xugurtp/2024/09/lyb/core-site.xml" "fs.defaultFS" = "s3a://xugurtp"
     *     "fs.s3a.access.key" = "1OkI53dJYgOJODLfUoQg" "fs.s3a.secret.key" =
     *     "V4ROTtoJTaLb0UI9VfgA6ZJM2FaNZBsIaBJyrtNW" "fs.s3a.endpoint" = "http://10.28.23.110:9010"
     *     "fs.s3a.path.style.access" = "true" "fs.s3a.impl" =
     *     "org.apache.hadoop.fs.s3a.S3AFileSystem" "fs.s3a.aws.credentials.provider" =
     *     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" "fs.s3a.connection.ssl.enabled" =
     *     "false" "hadoop.conf-local-tmp-dir" = "/tmp/seatunnel/11111111" }
     */
    public static String s3ToLocalPath(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.get(HiveConfig.S3_CONF).isEmpty()) {
            return readonlyConfig.get(HiveConfig.HADOOP_CONF_PATH);
        }
        Optional<Map<String, String>> s3HadoopConfPath =
                readonlyConfig.getOptional(HiveConfig.S3_CONF);

        Configuration hadoopConf = new Configuration();
        // 将 S3 的配置信息添加到 Hadoop 配置中
        s3HadoopConfPath.ifPresent(
                conf -> {
                    for (Map.Entry<String, String> entry : conf.entrySet()) {
                        hadoopConf.set(entry.getKey(), entry.getValue());
                    }
                });
        String s3HiveSitePath = hadoopConf.get("hive.s3_hive_site_path");
        String s3HdfsSitePath = hadoopConf.get("hive.s3_hdfs_site_path");
        String s3CoreSitePath = hadoopConf.get("hive.s3_core_site_path");

        String bucketName = hadoopConf.get("fs.defaultFS");
        String totalPath = hadoopConf.get("hadoop.conf-local-tmp-dir");
        String krb5Path = hadoopConf.get("hive.s3-krb5_path");
        String krb5KeyTab = hadoopConf.get("hive.s3-krb5_keytab");
        // 先处理kerberos文件
        if (krb5Path != null) {
            Path s3Krb5Path = new Path(bucketName + krb5Path);
            try (FileSystem fs = FileSystem.get(new URI(bucketName + krb5Path), hadoopConf)) {
                // 通过 FileSystem 下载 S3 文件到本地
                fs.copyToLocalFile(s3Krb5Path, new Path(totalPath + "/" + "krb5.conf"));
            } catch (Exception e) {
                throw new SeaTunnelException("获取s3的krb5.conf文件失败", e);
            }
        }
        if (krb5KeyTab != null) {
            Path s3Krb5KeyTabPath = new Path(bucketName + krb5KeyTab);
            try (FileSystem fs = FileSystem.get(new URI(bucketName + krb5KeyTab), hadoopConf)) {
                // 通过 FileSystem 下载 S3 文件到本地
                fs.copyToLocalFile(s3Krb5KeyTabPath, new Path(totalPath + "/" + "hive.keytab"));
            } catch (Exception e) {
                throw new SeaTunnelException("获取s3的krb5.keytab文件失败", e);
            }
        }
        // 检查 S3 文件列表和 Bucket 名称是否存在
        if (s3HiveSitePath != null) {
            Path s3HiveSitePathPath = new Path(bucketName + s3HiveSitePath);
            try (FileSystem fs = FileSystem.get(new URI(bucketName + s3HiveSitePath), hadoopConf)) {
                // 通过 FileSystem 下载 S3 文件到本地
                fs.copyToLocalFile(s3HiveSitePathPath, new Path(totalPath + "/" + "hive-site.xml"));
            } catch (Exception e) {
                throw new SeaTunnelException("获取s3的hive-site.xml文件失败", e);
            }
        }
        if (s3HdfsSitePath != null) {
            Path s3HdfsSitePathPath = new Path(bucketName + s3HdfsSitePath);
            try (FileSystem fs = FileSystem.get(new URI(bucketName + s3HdfsSitePath), hadoopConf)) {
                // 通过 FileSystem 下载 S3 文件到本地
                fs.copyToLocalFile(s3HdfsSitePathPath, new Path(totalPath + "/" + "hdfs-site.xml"));
            } catch (Exception e) {
                throw new SeaTunnelException("获取s3的hdfs-site.xml文件失败", e);
            }
        }

        if (s3CoreSitePath != null) {
            Path s3CoreSitePathPath = new Path(bucketName + s3CoreSitePath);
            try (FileSystem fs = FileSystem.get(new URI(bucketName + s3CoreSitePath), hadoopConf)) {
                // 通过 FileSystem 下载 S3 文件到本地
                fs.copyToLocalFile(s3CoreSitePathPath, new Path(totalPath + "/" + "core-site.xml"));
            } catch (Exception e) {
                throw new SeaTunnelException("获取s3的core-site.xml文件失败", e);
            }
        }

        // 本地文件路径列表
        return totalPath;
    }
}
