env {
	job.mode = "BATCH"
	parallelism = "1"
	job.retry.times = "0"
}

source {
	MappingJdbc {
		result_table_name = "tmp_table_jdbc_12034"
		query = """select * from SYSDBA.YYY"""
		parallelism = 1
		fetch_size = 3000
		password_decrypt = true
		table_path = "testldj.SYSDBA.YYY"
		partition_num = 1
		driver = "com.xugu.cloudjdbc.Driver"
		url = "jdbc:xugu://192.168.2.237:5190/testldj"
		user = "SYSDBA"
		password = "hyjI/9V6YZ/Hf65Vx9RZgXGiR7dhSbUxxNszRNNO3pz+t5MNzsyKZnaGfky1eLTxgnHVwWubxkrqUUEtYBd1UzIFuEOHlqQVgCId4MPi65JQr+HFaeVvooDGRDwxu5k253plsVefaCYPpK590qbe/SzIemSwBLEDs9DK5LdjSHo="
	}
}

transform {
}

sink {
	MappingJdbc {
		database = "TESTLDJ"
		table = "SYSDBA.YYY"
		batch_size = 3000
		generate_sink_sql = true
		parallelism = 1
		field_mapper = {"ID":"ID","NUM":"NUM","CONTENT":"CONTENT"}
		source_table_name = "tmp_table_jdbc_12034"
		password_decrypt = true
		data_save_mode = "APPEND_DATA"
		schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
		enable_upsert = false
		driver = "com.xugu.cloudjdbc.Driver"
		url = "jdbc:xugu://192.168.2.237:5190/testldj"
		user = "SYSDBA"
		password = "hyjI/9V6YZ/Hf65Vx9RZgXGiR7dhSbUxxNszRNNO3pz+t5MNzsyKZnaGfky1eLTxgnHVwWubxkrqUUEtYBd1UzIFuEOHlqQVgCId4MPi65JQr+HFaeVvooDGRDwxu5k253plsVefaCYPpK590qbe/SzIemSwBLEDs9DK5LdjSHo="
	}
}