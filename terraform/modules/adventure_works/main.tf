terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.62.1"
    }
  }
}

resource "databricks_catalog" "ctr" {
  name        = "bruno_vieira_ctr"
  comment     = "Área de controle da AdventureWorks"
  owner       = "bruno.vieira@indicium.tech"
}

resource "databricks_catalog" "raw" {
  name        = "bruno_vieira_raw"
  comment     = "Área de dados brutos da AdventureWorks"
  owner       = "bruno.vieira@indicium.tech"
}

resource "databricks_catalog" "stg" {
  name        = "bruno_vieira_stg"
  comment     = "Área de staging da AdventureWorks"
  owner       = "bruno.vieira@indicium.tech"
}