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
    file_filter_pattern  = "type5.*"
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
    table_name = "test.type2"
    metastore_uri = "thrift://xg-chuanwei-node2:9083"
    hdfs_site_path = "D:/安装包/kerberos/hive认证/hdfs-site.xml"
    hive_site_path = "D:/安装包/kerberos/hive认证/hive-site.xml"
    kerberos_principal = "hive/xg-chuanwei-node2@HADOOP.COM"
    krb5_path = "D:/安装包/kerberos/hive认证/krb5.conf"
    kerberos_keytab_path = "D:/安装包/kerberos/hive认证/hive.service.keytab"
  }
}
