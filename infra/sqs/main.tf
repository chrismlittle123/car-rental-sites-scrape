module "project_sqs_queue" {
  source       = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/sqs"
  project      = var.project
  service_name = "lambda"
}
