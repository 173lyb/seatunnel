env {
  parallelism = "2"
  job.mode = "BATCH"
  #checkpoint.interval = 100000
}

source {
	MappingJdbc {
		url = "jdbc:xugu://10.28.23.110:5138/SYSTEM?batch_mode=true"
		driver = "com.xugu.cloudjdbc.Driver"
		connection_check_timeout_sec = 100
		user = "SYSDBA"
		password = "SYSDBA"
		password_decrypt = false
        partition_num = 4
        fetch_size = 5000
        partition_column = "ID"
		#table_path是走catalog的sql语句拿列信息的
		table_path = "SYSDBA.TEST_KEY_CON1"
		#query是走jdbc拿列信息的
		query = "select * from SYSDBA.TEST_KEY_CON1 where ID >= 1"
	}
}
sink {
    MappingJdbc {
        url = "jdbc:xugu://10.28.23.110:5138/SYSTEM"
        driver = "com.xugu.cloudjdbc.Driver"
        user = "SYSDBA"
        field_mapper = {"ID":"ID","A":"B","B":"A","C":"C"}
        password = "SYSDBA"
        password_decrypt = false
        generate_sink_sql = true
        primary_keys = ["ID"]
        enable_upsert = true
        data_save_mode = "APPEND_DATA"
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        database = SYSTEM
        table = SYSDBA.TEST_KEY_CON1_AUTO2
        batch_size = 10000
        }
}
