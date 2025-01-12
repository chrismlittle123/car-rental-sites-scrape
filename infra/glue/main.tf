locals {
  columns_data = jsondecode(file("../src/aws_lambda/api/models/table_schemas.json"))

  do_you_spain_raw_columns        = local.columns_data.do_you_spain_raw.columns
  do_you_spain_raw_partition_keys = local.columns_data.do_you_spain_raw.partition_keys

  holiday_autos_raw_columns        = local.columns_data.holiday_autos_raw.columns
  holiday_autos_raw_partition_keys = local.columns_data.holiday_autos_raw.partition_keys

  rental_cars_raw_columns        = local.columns_data.rental_cars_raw.columns
  rental_cars_raw_partition_keys = local.columns_data.rental_cars_raw.partition_keys

  processed_columns        = local.columns_data.processed.columns
  processed_partition_keys = local.columns_data.processed.partition_keys

}

resource "aws_glue_catalog_database" "do_you_spain" {
  name = "do_you_spain"
}

resource "aws_glue_catalog_database" "holiday_autos" {
  name = "holiday_autos"
}

resource "aws_glue_catalog_database" "rental_cars" {
  name = "rental_cars"
}

resource "aws_athena_workgroup" "project_workgroup" {
  name = "${var.project}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.project}-bucket-${var.aws_account_id}/athena_results/"
    }
  }
}

module "do_you_spain_raw" {
  source         = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/glue_table"
  database_name  = aws_glue_catalog_database.do_you_spain.name
  table_name     = "raw"
  aws_account_id = var.aws_account_id
  project        = var.project
  columns        = local.do_you_spain_raw_columns
  partition_keys = local.do_you_spain_raw_partition_keys
}

module "do_you_spain_processed" {
  source         = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/glue_table"
  database_name  = aws_glue_catalog_database.do_you_spain.name
  table_name     = "processed"
  aws_account_id = var.aws_account_id
  project        = var.project
  columns        = local.processed_columns
  partition_keys = local.processed_partition_keys

}

module "holiday_autos_raw" {
  source         = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/glue_table"
  database_name  = aws_glue_catalog_database.holiday_autos.name
  table_name     = "raw"
  aws_account_id = var.aws_account_id
  project        = var.project
  columns        = local.holiday_autos_raw_columns
  partition_keys = local.holiday_autos_raw_partition_keys
}

module "holiday_autos_processed" {
  source         = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/glue_table"
  database_name  = aws_glue_catalog_database.holiday_autos.name
  table_name     = "processed"
  aws_account_id = var.aws_account_id
  project        = var.project
  columns        = local.processed_columns
  partition_keys = local.processed_partition_keys
}

module "rental_cars_raw" {
  source         = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/glue_table"
  database_name  = aws_glue_catalog_database.rental_cars.name
  table_name     = "raw"
  aws_account_id = var.aws_account_id
  project        = var.project
  columns        = local.rental_cars_raw_columns
  partition_keys = local.rental_cars_raw_partition_keys
}

module "rental_cars_processed" {
  source         = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/glue_table"
  database_name  = aws_glue_catalog_database.rental_cars.name
  table_name     = "processed"
  aws_account_id = var.aws_account_id
  project        = var.project
  columns        = local.processed_columns
  partition_keys = local.processed_partition_keys
}
