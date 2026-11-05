--ADD JAR /opt/hive/lib/opencsv-5.9.jar;

DROP TABLE IF EXISTS sales_stats;

CREATE EXTERNAL TABLE sales_stats
(product_id STRING, transaction_year INT, total_quantity BIGINT, avg_unit_price DOUBLE, mobile_quantity BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location '${input_dir3}';

DROP TABLE IF EXISTS products_data;

CREATE EXTERNAL TABLE products_data
(product_id STRING, name STRING, category STRING, brand STRING, supplier STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"'
)
STORED AS TEXTFILE location '${input_dir4}'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS result_json;
CREATE EXTERNAL TABLE result_json (
    category STRING,
    total_quantity BIGINT,
    avg_unit_price DOUBLE,
    mobile_quantity_total BIGINT,
    brands_ranking ARRAY<STRUCT<brand:STRING, rank_in_category:INT>>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${output_dir6}';

WITH category_and_brand AS (
    SELECT
        p.category as category,
        p.brand as brand,
        sum(s.total_quantity) as total_quantity,
        sum(s.avg_unit_price * s.total_quantity) / sum(s.total_quantity) AS avg_unit_price,
        sum(s.mobile_quantity) as mobile_quantity
    FROM sales_stats s JOIN products_data p ON s.product_id = p.product_id
    GROUP BY p.category, p.brand
    ),
    ranked AS(
        SELECT *, RANK() OVER (PARTITION BY category ORDER BY total_quantity desc) ranking
        FROM category_and_brand
    )

INSERT OVERWRITE TABLE result_json

SELECT
    category,
    sum(total_quantity) as total_quantity,
    round(sum(avg_unit_price * total_quantity) / sum(total_quantity), 3) AS avg_unit_price,
    sum(mobile_quantity) as mobile_quantity_total,
    collect_list(named_struct('brand', brand, 'rank_in_category', ranking))
FROM ranked
GROUP BY category;
