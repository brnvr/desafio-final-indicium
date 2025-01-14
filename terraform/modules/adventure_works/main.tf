terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.62.1"
    }
  }
}

resource "databricks_catalog" "stg" {
  name        = "bruno_vieira_stg"
  comment     = "Área de staging da AdventureWorks"
  properties  = {
    purpose = "teste"
  }
}

resource "databricks_schema" "stg_sales" {
  catalog_name = "bruno_vieira_stg"
  name         = "sales"
  depends_on    = [databricks_catalog.stg]
}

data "databricks_table" "stg_sales_CountryRegionCurrency" {
  name = "${databricks_catalog.stg.name}.sales.CountryRegionCurrency"
}