create table if NOT EXISTS {env}{catalog}{schema}example as select * from {source_catalog}{source_schema}trips;