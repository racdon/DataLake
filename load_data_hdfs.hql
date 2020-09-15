LOAD DATA INPATH '${hiveconf:hdfs_loc}' OVERWRITE INTO TABLE ${hiveconf:database}.${hiveconf:table};

select * from ${hiveconf:database}.${hiveconf:table};
