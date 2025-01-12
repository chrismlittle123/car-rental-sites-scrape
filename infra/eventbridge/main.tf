module "process_data_lambda_eventbridge" {
  source               = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/eventbridge"
  lambda_function_name = "process-data-lambda"
  event_pattern = jsonencode({
    "source" : ["com.oxforddataprocesses"],
    "detail-type" : ["S3PutObject"]
  })
  aws_account_id = var.aws_account_id
  aws_region     = var.aws_region
  project        = var.project
}
