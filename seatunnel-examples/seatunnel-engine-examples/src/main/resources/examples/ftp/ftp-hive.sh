env {
	job.mode = "batch"
	parallelism = "1"
	job.retry.times = "0"
	job.name = "aace8bb9f8864562b0264ea75e3991f5"
	checkpoint.interval = "1000"
}

source {

  FtpFile {
    result_table_name = "tmp_table_ftp_31ad8"
    path = "/home/236ftpuser/source-error"
    file_filter_list  = "type5-200.txt"
    host = "192.168.2.236"
    port = 21
    user = "236ftpuser"
    password = "xugu236ftp"
    file_format_type = "text"
    skip_header_row_number = 1
    schema = {
      fields {
        c_map = "string"
        c_array = "string"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(38, 18)"
        c_timestamp = timestamp
        }
    }
    #指定读取的列名，schema要按顺序跟文本对应

    #read_columns = ["c_decimal","c_string"]
    field_delimiter = ","
  }
}



sink {
  Hive {
   source_table_name = "hive1"
    table_name = "default.all_type"
    metastore_uri = "thrift://192.168.0.151:9083,thrift://192.168.0.193:9083"
    #hdfs_site_path = "D:/安装包/kerberos/hive认证/hdfs-site.xml"
    hive.hadoop.conf-path = "D:/安装包/华为云/FusionInsight_Cluster_1_Hive_Client/FusionInsight_Cluster_1_Hive_ClientConfig/FusionInsight_Cluster_1_Hive_ClientConfig/Hive/config"
    #hive_site_path = "D:/安装包/华为云/FusionInsight_Cluster_1_Hive_Client/FusionInsight_Cluster_1_Hive_ClientConfig/FusionInsight_Cluster_1_Hive_ClientConfig/Hive/config/hive-site.xml"
    kerberos_principal = "xugu@1DAD03C5_0944_4F3A_8B59_909FD022D2C5.COM"
    krb5_path = "D:/安装包/华为云/xugu_1723690816024_keytab/krb5.conf"
    kerberos_keytab_path = "D:/安装包/华为云/xugu_1723690816024_keytab/user.keytab"
  }
}
