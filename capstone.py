spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_claims=spark.read.option("header","true").json("s3://takeo123/capstone/claims.json")
df_claims.show()

df_claims.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.claims")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_disease=spark.read.option("header","true").csv("s3://takeo123/capstone/disease.csv")
df_disease.show()

df_disease = df_disease.withColumnRenamed(" Disease_ID", "Disease_ID")
df_disease.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.disease")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_group=spark.read.option("header","true").csv("s3://takeo123/capstone/group.csv")
df_group.show()

df_group.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.group")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_grpsubgrp=spark.read.option("header","true").csv("s3://takeo123/capstone/grpsubgrp.csv")
df_grpsubgrp.show()

df_grpsubgrp.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.grpsubgrp")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_hospital=spark.read.option("header","true").csv("s3://takeo123/capstone/hospital (2).csv")
df_hospital.show()

df_hospital.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.hospital")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_Patient_records=spark.read.option("header","true").csv("s3://takeo123/capstone/Patient_records.csv")
df_Patient_records.show()

df_Patient_records.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.Patient_records")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_subgroup=spark.read.option("header","true").csv("s3://takeo123/capstone/subgroup.csv")
df_subgroup.show()

df_subgroup.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.subgroup")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_subscriber=spark.read.option("header","true").csv("s3://takeo123/capstone/subscriber.csv")
df_subscriber.show()

df_subscriber=df_subscriber.withColumnRenamed("sub _id", "sub_id")
df_subscriber=df_subscriber.withColumnRenamed("Zip Code", "ZipCode")
df_subscriber.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.subscriber")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()