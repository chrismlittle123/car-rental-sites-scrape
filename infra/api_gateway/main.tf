module "api_gateway" {
  source                      = "git::https://github.com/Oxford-Data-Processes/terraform.git//modules/api_gateway"
  aws_account_id              = var.aws_account_id
  aws_region                  = var.aws_region
  stage                       = var.stage
  project                     = var.project
  aws_access_key_id_admin     = var.aws_access_key_id_admin
  aws_secret_access_key_admin = var.aws_secret_access_key_admin
  version_number              = var.version_number
}
