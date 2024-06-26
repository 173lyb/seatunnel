env {
	job.mode = "batch"
	parallelism = "6"
	job.retry.times = "0"
	job.name = "aace8bb9f8864562b0264ea75e3991f5"
	checkpoint.interval = "10000"
}

source {
	Kafka {
        schema = {
          fields {
            "Type": "string",
            "AppearTime": "timestamp",
            "Calling": "int",
            "DeviceID": "string",
            "Direction": "string",
            "DisappearTime": "timestamp",
            "HasPlate": "int",
            "InfoKind": "int",
            "LaneNo": "int",
            "LeftTopX": "int",
            "LeftTopY": "int",
            "MarkTime": "timestamp",
            "MotorVehicleID": "string",
            "PassTime": "timestamp",
            "PlateClass": "string",
            "PlateColor": "string",
            "PlateNo": "string",
            "RightBtmX": "int",
            "RightBtmY": "int",
            "SafetyBelt": "int",
            "SourceID": "string",
            "StorageUrl1": "string",
            "StorageUrl2": "string",
            "Sunvisor": "int",
            "TTL": "int",
            "TTL1From": "timestamp",
            "TTL1Time": "timestamp",
            "TTL2From": "timestamp",
            "TTL2Time": "timestamp",
            "TTL3From": "timestamp",
            "TTL3Time": "timestamp",
            "TollgateID": "string",
            "VehicleBrand": "string",
            "VehicleClass": "string",
            "VehicleColor": "string",
            "VehicleModel": "string",
            "VehicleStyles": "string",
            "datafrom": "string",
            "recvtime": "timestamp"
          }
        }
        format = "json"
		bootstrap.servers = "10.28.23.131:9092"
		format_error_handle_way = "skip"
		topic = "MotorVehicles"
		consumer.group = "1111"
		semantics = EXACTLY_ONCE
		start_mode = "earliest"
		result_table_name = "tmp_table_kafka_31ad8"
	}
}

transform {
}

sink {
	MappingJdbc {
		database = "postgres"
		table = "public.MotorVehicles"
		batch_size = 5000
		generate_sink_sql = true
		#primary_keys = ["ID"]
		source_table_name = "tmp_table_kafka_31ad8"
		data_save_mode = "APPEND_DATA"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		#enable_upsert = true
		driver = "com.postgresql.Driver"
		url = "jdbc:postgresql://10.28.23.162:5432/postgres"
		user = "postgres"
		password = "postgres"
	}
}