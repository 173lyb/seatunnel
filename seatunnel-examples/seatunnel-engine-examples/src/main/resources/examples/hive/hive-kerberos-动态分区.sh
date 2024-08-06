env {
	job.mode = "batch"
	parallelism = "1"
	job.retry.times = "0"
	job.name = "aace8bb9f8864562b0264ea75e3991f5"
	checkpoint.interval = "30000"
	checkpoint.timeout = "1233323"
}

source {
	Kafka {
        schema = {
          fields {
            id = bigint
            c_map = "map<string, smallint>"
            c_array = "array<tinyint>"
            c_string = string
            c_boolean = boolean
            c_tinyint = tinyint
            c_smallint = smallint
            c_int = int
            c_bigint = bigint
            c_float = float
            c_double = double
            c_decimal = "decimal(7, 2)"
            c_bytes = bytes
            c_date = date
          }
        }
        format = "JSON"
		bootstrap.servers = "10.28.23.131:9092"
		format_error_handle_way = "skip"
		topic = "type1"
		consumer.group = "1111"
		semantics = EXACTLY_ONCE
		start_mode = "earliest"
		result_table_name = "hive1"
	}
}
sink {
  # choose stdout output plugin to output data to console

  Hive {
   source_table_name = "hive1"
    table_name = "test.type1_par2"
    metastore_uri = "thrift://xg-chuanwei-node2:9083"
    #hive.hadoop.conf-path = "D:/安装包/kerberos/hive认证"
    hdfs_site_path = "D:/安装包/kerberos/hive认证/hdfs-site.xml"
    hive_site_path = "D:/安装包/kerberos/hive认证/hive-site.xml"
    kerberos_principal = "hive/xg-chuanwei-node2@HADOOP.COM"
    krb5_path = "D:/安装包/kerberos/hive认证/krb5.conf"
    kerberos_keytab_path = "D:/安装包/kerberos/hive认证/hive.service.keytab"
  }
}
