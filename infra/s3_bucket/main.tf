module "project_s3_bucket" {
  source         = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/s3_bucket"
  bucket_name    = "${var.project}-bucket-${var.aws_account_id}"
  aws_account_id = var.aws_account_id
}
