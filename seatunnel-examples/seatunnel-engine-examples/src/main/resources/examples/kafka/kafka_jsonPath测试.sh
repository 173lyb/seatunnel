env {
	job.mode = "batch"
	parallelism = "1"
	job.retry.times = "0"
	job.name = "aace8bb9f8864562b0264ea75e3991f5"
	checkpoint.interval = "30000"
}

source {
	Kafka {

        json_field = {
         fields = {
           "key1" = "$.Slice[*].Type"
           "key2" = "$.Slice[*].Req.MotorVehicleListObject.MotorVehicleObject[*].Direction"
           "key3" = "$.Slice[*].Req.MotorVehicleListObject.MotorVehicleObject[*].StorageUrl1"
           "key4" = "$.Slice[*].Req.MotorVehicleListObject.MotorVehicleObject[*].AppearTime"
           "key5" = "$.Slice[*].Req.MotorVehicleListObject.MotorVehicleObject[*].SubImageList.SubImageInfoObject[*].EventSort"
           }
        }
        schema = {
          fields = {
            key1 = "string"
            key2 = "string"
            key3 = "string"
            key4 = "string"
            key5 = "string"
          }
        }
        format = "JSON"
		bootstrap.servers = "10.28.23.131:9092"
		format_error_handle_way = "skip"
		topic = "MotorVehicles"
		consumer.group = "1111"
		semantics = EXACTLY_ONCE
		start_mode = "earliest"
		result_table_name = "tmp_table_kafka_31ad8"
	}
}


sink {
	Console {
		source_table_name = "tmp_table_kafka_31ad8"
	}
}
