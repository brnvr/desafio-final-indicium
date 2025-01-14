provider "databricks" {
  host  = "https://${var.databricks_instance}.azuredatabricks.net"
  token = var.databricks_token
}