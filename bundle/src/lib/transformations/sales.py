from pyspark.sql import DataFrame
from databricks.sdk.runtime import *
from pyspark.sql.functions import from_xml

def transform_credit_cards(df:DataFrame):
    return df.withColumnsRenamed({
        "ExpMonth": "expiration_month",
        "ExpYear": "expiration_year"
    })

def transform_order_details(df:DataFrame):
    return df.withColumnsRenamed({
        "OrderQty": "order_quantity"
    })

def transform_order_headers(df:DataFrame):
    return df.withColumnsRenamed({
        "TaxAmt": "tax_amount"
    })

def transform_sales_people(df:DataFrame):
    return df.withColumnsRenamed({
        "CommissionPct": "commission_percentage"
    })

def transform_special_offers(df:DataFrame):
    return df.withColumnsRenamed({
        "MinQty": "min_quantity",
        "MaxQty": "max_quantity",
        "DiscountPct": "discount_percentage"
    })

def transform_stores(df:DataFrame):
    return df.select([col for col in df.columns if col != "Demographics"])

def transform_store_demographics(df:DataFrame):
    #demographics_schema = schema_of_xml(df.first()["Demographics"])
    demographics_schema = """
        AnnualRevenue DECIMAL(18,2),
        AnnualSales INT,
        BankName STRING,
        Brands STRING,
        BusinessType STRING,
        Internet STRING,
        NumberEmployees INT,
        Specialty STRING,
        SquareFeet INT,
        YearOpened INT
    """

    return (df
        .withColumn(
            "Demographics",
            from_xml("Demographics", demographics_schema).alias("data")
        )
        .select(
            "BusinessEntityID",
            "Demographics.AnnualRevenue",
            "Demographics.AnnualSales",
            "Demographics.BankName",
            "Demographics.Brands",
            "Demographics.BusinessType",
            "Demographics.Internet",
            "Demographics.NumberEmployees",
            "Demographics.Specialty",
            "Demographics.SquareFeet",
            "Demographics.YearOpened",
        )
        .withColumnRenamed("NumberEmployees", "number_of_employees")
    )