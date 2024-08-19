env {
	job.mode = "STREAMING"
	parallelism = "1"
	job.retry.times = "0"
	job.name = "aace8bb9f8864562b0264ea75e3991f5"
	checkpoint.interval = "180000"
}

source {
	Kafka {
        schema = {
          fields {
            data = "string"
          }
        }
		format = "json"
		bootstrap.servers = "192.168.0.210:21007,192.168.0.214:21007,192.168.0.233:21007"
		topic = "example-metric1"
		consumer.group = "111"
		start_mode = "earliest"
		field_delimiter = ","
        krb5_path = "D:/krb5.conf"
        kerberos_principal = "xugu"
        kerberos_keytab_path = "D:/huawei-xugu.keytab"
        jaas.conf = {
            com.sun.security.auth.module.Krb5LoginModule = "required"
            serviceName = "kafka"
            useKeyTab = true
            storeKey = true
            useTicketCache = false
        }

        kafka.config = {
            security.protocol = SASL_PLAINTEXT
            sasl.kerberos.service.name = kafka
            sasl.kerberos.kinit.cmd = "kinit"
            sasl.mechanism=GSSAPI
        }
		result_table_name = "tmp_table_kafka_31ad8"
	}
}

transform {
}

sink {
Console {
        source_table_name = "tmp_table_kafka_31ad8"
}
}