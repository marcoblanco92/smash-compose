DROP TABLE IF EXISTS smash_olap.stg_transactions_kafka;

CREATE TABLE smash_olap.stg_transactions_kafka
(
    raw_json String
)
    ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'smash.smash_own.transactions',
    kafka_group_name = 'clickhouse-consumer',
    kafka_format = 'JSONAsString';