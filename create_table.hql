use ${hiveconf:database};


create ${hiveconf:creation_type} table ${hiveconf:table} ( ${hiveconf:col_attr_syntax} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

describe formatted ${hiveconf:table};
