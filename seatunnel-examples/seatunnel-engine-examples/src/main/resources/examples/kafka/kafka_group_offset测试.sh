env {
	job.mode = "streaming"
	parallelism = "6"
	job.retry.times = "0"
	job.name = "aace8bb9f8864562b0264ea75e3991f5"
	checkpoint.interval = "10000"
}

source {
	Kafka {

        json_field = {
         fields = {
           "col_7" = "$.Slice[*].Type"
           }
        }
        schema = {
          fields = {
            col_7 = "string"
          }
        }
        format = "JSON"
		bootstrap.servers = "10.28.23.131:9092"
		format_error_handle_way = "skip"
		topic = "MotorVehicles-3"
		consumer.group = "11111-2111133"
		semantics = EXACTLY_ONCE
		start_mode = "latest"
		commit_on_checkpoint = true
        kafka.config = {
            auto.offset.reset = latest
            enable.auto.commit = false
            auto.commit.interval.ms = 2000
            max.poll.interval.ms = 300000
            max.poll.records = 500
            fetch.max.bytes = 52428800
            fetch.max.wait.ms = 500
        }
		result_table_name = "tmp_table_kafka_31ad8"
	}
}

transform{
    sql{
        source_table_name = "tmp_table_kafka_31ad8"
        result_table_name = "tmp_table_kafka_31ad8-1"
        query="select 1 as id ,col_7 from tmp_table_kafka_31ad8"
    }
}

sink {
	Jdbc {
		database = "d1"
		table = "u1.type1"
		batch_size = 5000
		generate_sink_sql = true
		#primary_keys = ["id"]
		source_table_name = "tmp_table_ftp_31ad8-1"
		data_save_mode = "APPEND_DATA"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		#enable_upsert = true
		driver = "org.postgresql.Driver"
		url = "jdbc:postgresql://192.168.2.195:15400/d1"
		user = "u1"
		password = "123456"
		gauss_support_merge = true
	}
}
