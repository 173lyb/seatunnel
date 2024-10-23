env {
	job.mode = "BATCH"
	parallelism = "1"
	job.retry.times = "0"
	job.name = "1837085933850710017"
}

source {
	S3File {
		fs.s3a.endpoint = "http://10.28.23.110:9010"
		fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
		access_key = "admin"
		secret_key = "12345678"
		bucket = "s3a://xugurtp"
		path = "/tmp/example_data.json"
		delimiter = ""
		file_format_type = "json"
		result_table_name = "tmp_table_file_40674"
		xml_use_attr_format = ""
		xml_row_tag = ""
		sheet_name = ""
		encoding = "UTF-8"
		schema = {"fields":{"a":"string","s":"string","d":"string","f":"string","g":"timestamp","j":"string","k":"string","l":"string"}}
		json_field = {
                   a = "$.rows[*].workspaceId"
                   s = "$.rows[*].workspaceName"
                   d = "$.rows[*].description"
                   f = "$.rows[*].createBy"
                   g = "$.rows[*].createTime"
                   j = "$.rows[*].status"
                   k = "$.rows[*].tenantId"
                   l = "$.rows[*].tenantName"
                }
	}
}

transform {
}

sink {
	Console {}
}