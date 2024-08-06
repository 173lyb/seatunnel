env {
	job.mode = "batch"
	parallelism = "3"
	job.retry.times = "0"
	job.name = "aace8bb9f8864562b0264ea75e3991f5"
}

source {

  FtpFile {
    result_table_name = "tmp_table_ftp_31ad8"
    path = "/home/236ftpuser/success"
    file_filter_pattern  = "type5-1.txt|type5-2.txt|type5-2 - 副本.txt"
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
	Jdbc {
		database = "postgres"
		table = "public.type1"
		batch_size = 5000
		generate_sink_sql = true
		#primary_keys = ["ID"]
		source_table_name = "tmp_table_ftp_31ad8-1"
		data_save_mode = "DROP_DATA"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		#enable_upsert = true
		driver = "org.postgresql.Driver"
		url = "jdbc:postgresql://10.28.23.162:5432/postgres"
		user = "postgres"
		password = "postgres"
	}
}
