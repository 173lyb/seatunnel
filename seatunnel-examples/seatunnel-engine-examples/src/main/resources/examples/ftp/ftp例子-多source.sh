env {
	job.mode = "batch"
	parallelism = "1"
	job.retry.times = "0"
	job.name = "aace8bb9f8864562b0264ea75e3991f5"
}

source {

  FtpFile {
    result_table_name = "tmp_table_ftp_31ad8"
    path = "/home/236ftpuser/type2"
    file_filter_pattern  = "type.*"
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
        seatunnel_ftp_file_path = string
        }
    }
    #指定读取的列名，schema要按顺序跟文本对应

    #read_columns = ["c_decimal","c_string"]
    field_delimiter = ","
  }
}

transform{
    sql = {
        source_table_name = "tmp_table_ftp_31ad8"
        result_table_name = "tmp_table_ftp_31ad8-1"
        query = "select c_map,c_array,c_string,c_boolean,c_tinyint,c_smallint,c_int,c_bigint,c_float,c_double,c_bytes,c_date,c_decimal,c_timestamp from tmp_table_ftp_31ad8"
    }
    sql = {
        source_table_name = "tmp_table_ftp_31ad8"
        result_table_name = "tmp_table_ftp_31ad8-2"
        query = "select seatunnel_ftp_file_path , NOW() as update_time  from tmp_table_ftp_31ad8"
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
		data_save_mode = "APPEND_DATA"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		#enable_upsert = true
		driver = "org.postgresql.Driver"
		url = "jdbc:postgresql://10.28.23.162:5432/postgres"
		user = "postgres"
		password = "postgres"
	}
	Jdbc {
		database = "postgres"
		table = "public.log1"
		batch_size = 5000
		generate_sink_sql = true
		primary_keys = ["seatunnel_ftp_file_path"]
		source_table_name = "tmp_table_ftp_31ad8-2"
		data_save_mode = "APPEND_DATA"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		enable_upsert = true
		driver = "org.postgresql.Driver"
		url = "jdbc:postgresql://10.28.23.162:5432/postgres"
		user = "postgres"
		password = "postgres"
	}
}
