spark-submit --driver-class-path $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.43.1067.jar \
--jars $SPARK_HOME/jars/RedshiftJDBC42-no-awssdk-1.2.43.1067.jar \
$PWD/dags/python_scripts/{{params.python_script}} {{ params.s3_bucket }} {{ params.s3_key }} \
{{ params.aws_key }} {{ params.aws_secret_key }} {{ params.redshift_conn_string }} \
{{ params.db_user }} {{params.db_pass}} --conf "fs.s3a.multipart.size=104857600"'