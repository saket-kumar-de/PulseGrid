resource "aws_glue_catalog_table" "curated_sensor_readings" {
  name          = "sensor_readings"
  database_name = aws_glue_catalog_database.curated.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "projection.dt.range"         = "2026-01-01,NOW"
    "projection.hour.type"        = "integer"
    "projection.hour.range"       = "0,23"
    "projection.hour.digits"      = "2"
    "storage.location.template"   = "s3://${aws_s3_bucket.curated.bucket}/sensor_readings/dt=$${dt}/hour=$${hour}/"
    "classification"              = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.curated.bucket}/sensor_readings/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "device_id"
      type = "string"
    }
    columns {
      name = "device_type"
      type = "string"
    }
    columns {
      name = "facility_id"
      type = "string"
    }
    columns {
      name = "zone"
      type = "string"
    }
    columns {
      name = "battery_pct"
      type = "double"
    }
    columns {
      name = "status_code"
      type = "string"
    }
    columns {
      name = "firmware_version"
      type = "string"
    }
    columns {
      name = "temperature_c"
      type = "double"
    }
    columns {
      name = "humidity_pct"
      type = "double"
    }
    columns {
      name = "vibration_mm_s"
      type = "double"
    }
    columns {
      name = "rpm"
      type = "double"
    }
    columns {
      name = "door_open_count"
      type = "double"
    }
    columns {
      name = "energy_kwh"
      type = "double"
    }
    columns {
      name = "voltage"
      type = "double"
    }
    columns {
      name = "event_ts"
      type = "timestamp"
    }
  }
}

resource "aws_glue_catalog_table" "quarantine_sensor_readings" {
  name          = "quarantine_sensor_readings"
  database_name = aws_glue_catalog_database.curated.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "projection.dt.range"         = "2026-01-01,NOW"
    "projection.hour.type"        = "integer"
    "projection.hour.range"       = "0,23"
    "projection.hour.digits"      = "2"
    "storage.location.template"   = "s3://${aws_s3_bucket.curated.bucket}/quarantine/sensor_readings/dt=$${dt}/hour=$${hour}/"
    "classification"              = "json"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.curated.bucket}/quarantine/sensor_readings/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "paths" = "battery_pct,device_id,device_type,door_open_count,energy_kwh,event_ts,facility_id,firmware_version,humidity_pct,rpm,status_code,temperature_c,timestamp,vibration_mm_s,voltage,zone"
      }
    }

    columns {
      name = "device_id"
      type = "string"
    }
    columns {
      name = "device_type"
      type = "string"
    }
    columns {
      name = "facility_id"
      type = "string"
    }
    columns {
      name = "zone"
      type = "string"
    }
    columns {
      name = "timestamp"
      type = "string"
    }
    columns {
      name = "battery_pct"
      type = "double"
    }
    columns {
      name = "status_code"
      type = "string"
    }
    columns {
      name = "firmware_version"
      type = "string"
    }
    columns {
      name = "temperature_c"
      type = "double"
    }
    columns {
      name = "door_open_count"
      type = "double"
    }
    columns {
      name = "event_ts"
      type = "string"
    }
    columns {
      name = "vibration_mm_s"
      type = "double"
    }
    columns {
      name = "rpm"
      type = "double"
    }
    columns {
      name = "energy_kwh"
      type = "double"
    }
    columns {
      name = "voltage"
      type = "double"
    }
    columns {
      name = "humidity_pct"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "pipeline_runs" {
  name          = "pipeline_runs"
  database_name = aws_glue_catalog_database.curated.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "projection.dt.range"         = "2026-01-01,NOW"
    "storage.location.template"   = "s3://${aws_s3_bucket.curated.bucket}/audit/pipeline_runs/dt=$${dt}/"
    "classification"              = "json"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.curated.bucket}/audit/pipeline_runs/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "paths" = "completed_at,duration_seconds,hours_processed,records_clean,records_quarantined,run_id,run_timestamp,started_at,status,target_date"
      }
    }

    columns {
      name = "run_id"
      type = "string"
    }
    columns {
      name = "run_timestamp"
      type = "string"
    }
    columns {
      name = "target_date"
      type = "string"
    }
    columns {
      name = "hours_processed"
      type = "string"
    }
    columns {
      name = "status"
      type = "string"
    }
    columns {
      name = "started_at"
      type = "string"
    }
    columns {
      name = "completed_at"
      type = "string"
    }
    columns {
      name = "duration_seconds"
      type = "double"
    }
    columns {
      name = "records_clean"
      type = "int"
    }
    columns {
      name = "records_quarantined"
      type = "int"
    }
  }
}