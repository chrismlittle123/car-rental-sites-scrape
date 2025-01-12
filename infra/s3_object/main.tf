locals {
  car_groups_path = "${path.module}/../../data/car_groups.csv"
  car_groups_hash = filemd5("${path.module}/../../data/car_groups.csv")
}

resource "aws_s3_object" "car_groups" {
  bucket = "${var.project}-bucket-${var.aws_account_id}"
  key    = "car_groups/car_groups.csv"
  source = local.car_groups_path

  etag = local.car_groups_hash

  lifecycle {
    create_before_destroy = true
  }
}
