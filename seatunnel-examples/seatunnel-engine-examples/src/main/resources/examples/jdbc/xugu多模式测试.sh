env {
	job.mode = "BATCH"
	parallelism = "1"
	job.retry.times = "0"
}

source {
	MappingJdbc {
		result_table_name = "tmp_table_jdbc_184f2"
		query = "select * from TEST1.TEST"
		parallelism = 1
		fetch_size = 3000
		password_decrypt = true
		table_path = "TEST1.TEST"
		partition_num = 1
		driver = "com.xugu.cloudjdbc.Driver"
		url = "jdbc:xugu://10.28.23.110:5138/system"
		user = "sysdba"
		password = "FgulyLo9GktJHHWg9ctrdn2nQzyxhWJJLOO5+6J2K1gtp1Kf2BrVhxSr6ukJ9KZhvMXFTCJNE+pSy7MuKmi5pSYw8o9fKbKin+LmrdcYbxA+hnggUNhiIqxWnroPcu+zjh8WI0BDi6EKEh6HV9vNPUUknNQAwpOO6UNkAn9kJFg="
	}
}

transform {
}

sink {
	MappingJdbc {
		database = "SYSTEM"
		table = "TEST1.TEST2"
		batch_size = 3000
		generate_sink_sql = true
		parallelism = 1
		field_mapper = {"ID":"ID"}
		source_table_name = "tmp_table_jdbc_184f2"
		password_decrypt = true
		data_save_mode = "APPEND_DATA"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		enable_upsert = false
		driver = "com.xugu.cloudjdbc.Driver"
		url = "jdbc:xugu://10.28.23.110:5138/system"
		user = "sysdba"
		password = "FgulyLo9GktJHHWg9ctrdn2nQzyxhWJJLOO5+6J2K1gtp1Kf2BrVhxSr6ukJ9KZhvMXFTCJNE+pSy7MuKmi5pSYw8o9fKbKin+LmrdcYbxA+hnggUNhiIqxWnroPcu+zjh8WI0BDi6EKEh6HV9vNPUUknNQAwpOO6UNkAn9kJFg="
	}
}