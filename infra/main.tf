terraform {
  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.70.0"
    }
  }

  required_version = ">= 1.1.0"
}

provider "aws" {
  region = var.aws_region
}

module "s3_bucket" {
  aws_account_id = var.aws_account_id
  project        = var.project
  source         = "./s3_bucket"
}

module "s3_object" {
  aws_account_id = var.aws_account_id
  project        = var.project
  source         = "./s3_object"
}

module "glue" {
  aws_account_id = var.aws_account_id
  project        = var.project
  source         = "./glue"
}

module "sqs" {
  project = var.project
  source  = "./sqs"
}

module "eventbridge" {
  aws_account_id = var.aws_account_id
  aws_region     = var.aws_region
  project        = var.project
  source         = "./eventbridge"
}

module "api_gateway" {
  aws_account_id              = var.aws_account_id
  aws_region                  = var.aws_region
  stage                       = var.stage
  project                     = var.project
  aws_access_key_id_admin     = var.aws_access_key_id_admin
  aws_secret_access_key_admin = var.aws_secret_access_key_admin
  version_number              = var.version_number
  source                      = "./api_gateway"
}

module "lambda" {
  aws_account_id              = var.aws_account_id
  aws_region                  = var.aws_region
  stage                       = var.stage
  project                     = var.project
  aws_access_key_id_admin     = var.aws_access_key_id_admin
  aws_secret_access_key_admin = var.aws_secret_access_key_admin
  version_number              = var.version_number
  source                      = "./lambda"
}
