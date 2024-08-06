env  {
  job.mode = "batch"
  parallelism = "1"
  job.retry.times = "0"
}
source  {
  FtpFile {
    host = "192.168.2.236"
    port = "21"
    user = "236ftpuser"
    password = "xugu236ftp"
    path = "/home/236ftpuser/"
    #file_filter_pattern  = "ab_cd-x,y-z+、《》，,中文.csv"
    file_filter_list = "ab_cd-x,y-z+、《》，,中文.csv"
    file_format_type = "CSV"
    result_table_name = "455aa65f-ec23-465f-9f2a-fd0967829ecf"
    skip_header_row_number = "0"
    connection_mode = "passive_local"
    schema = {
      fields = {
        id = "int"
        c2 = "string"
      }
    }
  }
}
sink  {
  Jdbc {
    database = "d2"
    table = "u2.tmp_t3"
    url = "jdbc:postgresql://192.168.2.195:15400/d2"
    user = "u2"
    password = "123456"
    driver = "org.postgresql.Driver"
    generate_sink_sql = "true"
    source_table_name = "455aa65f-ec23-465f-9f2a-fd0967829ecf"
    schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
    enable_upsert = "false"
  }
}