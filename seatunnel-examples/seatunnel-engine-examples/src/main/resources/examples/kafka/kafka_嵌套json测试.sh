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
            c_map = "map<string, string>"
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
            c_timestamp = timestamp
          }
        }
        format = "JSON"
		bootstrap.servers = "10.28.23.131:9092"
		format_error_handle_way = "skip"
		topic = "type1"
		consumer.group = "1111"
		semantics = EXACTLY_ONCE
		start_mode = "earliest"
		result_table_name = "tmp_table_kafka_31ad8"
	}
}

transform {
  JsonPath {
    source_table_name = "tmp_table_kafka_31ad8"
    result_table_name = "tmp_table_kafka_31ad8"
    columns = [
     {
        "src_field" = "c_map"
        "path" = "$.key1"
        "dest_field" = "c_map"
     }
    ]
  }
}

sink {
	Jdbc {
		database = "postgres"
		table = "public.type1"
		batch_size = 5000
		generate_sink_sql = true
		#primary_keys = ["ID"]
		source_table_name = "tmp_table_kafka_31ad8"
		data_save_mode = "APPEND_DATA"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		#enable_upsert = true
		driver = "org.postgresql.Driver"
		url = "jdbc:postgresql://10.28.23.162:5432/postgres"
		user = "postgres"
		password = "postgres"
	}
}
