# Databricks notebook source
# Load the cpsc_combined table into a DataFrame
cpsc_combined_df = spark.table("mimi_ws_1.partcd.cpsc_combined")

# Load the ssa2fips_state_and_county table into a DataFrame
ssa2fips_df = spark.table("mimi_ws_1.nber.ssa2fips_state_and_county")

# Perform the join and select the required columns
cpsc_combined_geo_df = cpsc_combined_df.alias('a').join(
    ssa2fips_df.alias('b'),
    cpsc_combined_df.ssa_state_county_code == ssa2fips_df.ssa_code,
    "left"
).select(
    "a.ssa_state_county_code", 
    "a.fips_state_county_code", 
    "a.state",
    "a.county",
    "a.contract_id",
    "a.plan_id",
    "a.enrollment",
    "a.organization_name",
    "a.organization_marketing_name",
    "a.plan_name",
    "a.parent_organization",
    "a.contract_effective_date",
    "b.fy2023cbsa",
    "b.fy2023cbsaname"
).withColumnRenamed("fy2023cbsa", "cbsa") \
 .withColumnRenamed("fy2023cbsaname", "cbsa_name")

display(cpsc_combined_geo_df)

# COMMAND ----------

# Load the geovariation table into a DataFrame
geovariation_df = spark.table("mimi_ws_1.datacmsgov.geovariation")

# Find the max year value
max_year = geovariation_df.agg({"year": "max"}).collect()[0][0]

# Filter the table to where year=max(year)
geovariation_max_year_df = geovariation_df.filter(geovariation_df.year == max_year)

display(geovariation_max_year_df)
