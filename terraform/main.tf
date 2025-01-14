terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.62.1"
    }
  }
}

module "adventure_works" {
  source        = "./modules/adventure_works"
}