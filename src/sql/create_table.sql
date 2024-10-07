CREATE TABLE IF NOT EXISTS `develop-431503.transportation_netherlands.ovapi` (
    id STRING,
    transport_type STRING,
    line_name STRING,
    line_public_number STRING,
    data_owner_code STRING,
    destination_name_50 STRING,
    line_planning_number STRING,
    line_direction STRING,
    load_date DATE,
    updated_at TIMESTAMP
)
PARTITION BY load_date 
CLUSTER BY transport_type, line_name, load_date;