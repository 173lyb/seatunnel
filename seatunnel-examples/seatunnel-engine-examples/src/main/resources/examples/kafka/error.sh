env {
	job.mode = "batch"
	parallelism = "1"
	job.retry.times = "0"
	checkpoint.interval = "10000"
	checkpoint.timeout = "1233323"
}
source  {
	Kafka {
		format = "JSON"
		bootstrap.servers = "10.28.23.131:9092"
		format_error_handle_way = "skip"
		topic = "test2"
		consumer.group = "1111"
		semantics = EXACTLY_ONCE
		start_mode = "earliest"
		result_table_name = "kafka_json"
		json_field = {
			fields  {
				storage_url1 = "$.Slice[*].Req.MotorVehicleListObject.MotorVehicleObject[*].StorageUrl1"
				storage_url2 = "$.Slice[*].Req.MotorVehicleListObject.MotorVehicleObject[*].StorageUrl2"
			}
		}
		schema = {
			fields {
				storage_url1 = "string"
				storage_url2 = "string"
			}
		}

	}
}
sink  {
	JDBC {
		database = "d1"
		table = "u1.motor_vehicles"
		url = "jdbc:postgresql://192.168.2.195:15400/d1"
		user = "u1"
		password = "123456"
		driver = "org.postgresql.Driver"
		generate_sink_sql = "true"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		data_save_mode = "APPEND_DATA"
		enable_upsert = "false"
	}
}