# Databricks notebook source
# MAGIC %md
# MAGIC <a href="https://www.genome.gov/nov3-2015-1000-genomes-project"><img src="https://www.genome.gov/sites/default/files/genome-old/images/content/1000genomes.jpg" width="400"></a>
# MAGIC
# MAGIC # Data Ingest and Trasnformation
# MAGIC
# MAGIC In this notebook, we load VCF files for chromosome 22 from `s3://1000genomes/phase1/` and create a database of variants, along with sample-level and variant-level summary statistics. Additionally, we incorporate sample metadata such as population of origin and call rate, and integrate a list of GWAS hits from the GWAS catalog. Subsequently, we generate a dashboard for interactive analysis of the results.
# MAGIC
# MAGIC to import the dashboard, 
# MAGIC
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNp1U8tu2zAQ_BVCubSAo5edtNahgCzRaYAUKOrUFykQaIm2CNOkSlJWjCDf0mMv_bh-Qley4kfhXoTV7OzOzpJ8sXJZUCuwVopUJXr4lgpdL_Y_szsPJXhy_5SKSfI4mweoNKbSgeMsTWV7ruuuqJAbqm26YDbJ7XrtbCX32rSTpsLQvBQsJ9xppFozsXJ81xu6t-5tpsmm4jRjYilb5iXcNs8GlKO9ctM0JyqrhmiHVMzRlKi8dArZCC5JoZ1lzTlUUVGcGfFRoocw-MnQTlUSTT0H2GEyj6YBykvl-_bsc_gV3z_6MIShUG9oke2p2Xb41vqs-RAlMTFkoVi-1k9nqRFKvgtmdigCAperVgxdX6M_P3_9RndcNgjcgziqiNJUQeoTCsNkBZmsGyfbQqI9gC41mSQnG1IbYpgU7ZK6bBQl7WKyfK-V9bsIW8WjYMuM41OJvuWP_DIZ4wvz_LvgG5Q8kDXdMtr0XexW592cKEaEQfi54lKBQ_CLYqLLhSSqeA_GJj0VXESHMI4PIcaHsBOFD-oOwOw47e7oknEeXMURdrF_xP3_4MMeH3_0XD8c5BLmCq6m4-mH6QgdaaOehjGO8fiI35zhUGANrA2Fo2AFvKIXcIdSy5R0Q1MrgLCgS1Jzk1qpeAVqXRVwo3DBjFRWYFRNBxapjZztRP72v-fEjMByN1awJFwDSruaL_vX2j3a17-e5EPD?type=png)](https://mermaid.live/edit#pako:eNp1U8tu2zAQ_BVCubSAo5edtNahgCzRaYAUKOrUFykQaIm2CNOkSlJWjCDf0mMv_bh-Qley4kfhXoTV7OzOzpJ8sXJZUCuwVopUJXr4lgpdL_Y_szsPJXhy_5SKSfI4mweoNKbSgeMsTWV7ruuuqJAbqm26YDbJ7XrtbCX32rSTpsLQvBQsJ9xppFozsXJ81xu6t-5tpsmm4jRjYilb5iXcNs8GlKO9ctM0JyqrhmiHVMzRlKi8dArZCC5JoZ1lzTlUUVGcGfFRoocw-MnQTlUSTT0H2GEyj6YBykvl-_bsc_gV3z_6MIShUG9oke2p2Xb41vqs-RAlMTFkoVi-1k9nqRFKvgtmdigCAperVgxdX6M_P3_9RndcNgjcgziqiNJUQeoTCsNkBZmsGyfbQqI9gC41mSQnG1IbYpgU7ZK6bBQl7WKyfK-V9bsIW8WjYMuM41OJvuWP_DIZ4wvz_LvgG5Q8kDXdMtr0XexW592cKEaEQfi54lKBQ_CLYqLLhSSqeA_GJj0VXESHMI4PIcaHsBOFD-oOwOw47e7oknEeXMURdrF_xP3_4MMeH3_0XD8c5BLmCq6m4-mH6QgdaaOehjGO8fiI35zhUGANrA2Fo2AFvKIXcIdSy5R0Q1MrgLCgS1Jzk1qpeAVqXRVwo3DBjFRWYFRNBxapjZztRP72v-fEjMByN1awJFwDSruaL_vX2j3a17-e5EPD)
# MAGIC

# COMMAND ----------

# MAGIC %pip install -r requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note:
# MAGIC To run this notebook you need to install Glow on your cluster. To do so, you need to [install](https://docs.databricks.com/en/libraries/cluster-libraries.html) `io.projectglow:glow-spark3_2.12:2.0.0` from Maven. 

# COMMAND ----------

## import glow and register glow functions in spark session
import glow
spark = glow.register(spark)

# COMMAND ----------

## import utility functions 
from utils.util import SolAccManager
solac = SolAccManager('./utils/config.yml')
params = solac.config
display(params)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add Chr22 Variant Data
# MAGIC [![](https://mermaid.ink/img/pako:eNpdkdFKwzAUhl_lEG8327UDXS6EsqZTUBCn3jSjZG3WFtOkJOlkjD2Ll974cD6Csd0YehNyzv-d8yfn7FGuCo4wKjVrK7h_opJK062HcLkIIDUh9ryJ7_sll6rhxmsrZvjEW1EZpa_zBENe6SC4XN5Gj-TuOchqabmrt7zIBjTbhg7msvjXPIQ0ZpatdZ2_mdUfaQrpi6ztDuYOEKr8NYPxGL4_Pr9gIdQ7UCqdObRMG66ddANRlJZOyfrnZFsnnFzdAb233Qne_2pTC4Ev4jnxSXDOh8f87HriB9EoV0JpfJHMkqtkCmdsesQIITGZoRFquG5YXbgx7qkEoMhWvOEUYXct-IZ1wlJE5cGhXVu4yZCitkojbHXHR4h1Vi13Mj_FAxPXzE2jQXjDhHFZ3tc8DOvqt3b4AYsflzc?type=png)](https://mermaid.live/edit#pako:eNpdkdFKwzAUhl_lEG8327UDXS6EsqZTUBCn3jSjZG3WFtOkJOlkjD2Ll974cD6Csd0YehNyzv-d8yfn7FGuCo4wKjVrK7h_opJK062HcLkIIDUh9ryJ7_sll6rhxmsrZvjEW1EZpa_zBENe6SC4XN5Gj-TuOchqabmrt7zIBjTbhg7msvjXPIQ0ZpatdZ2_mdUfaQrpi6ztDuYOEKr8NYPxGL4_Pr9gIdQ7UCqdObRMG66ddANRlJZOyfrnZFsnnFzdAb233Qne_2pTC4Ev4jnxSXDOh8f87HriB9EoV0JpfJHMkqtkCmdsesQIITGZoRFquG5YXbgx7qkEoMhWvOEUYXct-IZ1wlJE5cGhXVu4yZCitkojbHXHR4h1Vi13Mj_FAxPXzE2jQXjDhHFZ3tc8DOvqt3b4AYsflzc)

# COMMAND ----------

solac.add_schema(params['variant_schema_name'])

# COMMAND ----------

source = params['vcf_path']
destination = f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_vars"

spark.read.format("vcf")\
.option("includeSampleIds", True)\
.option("flattenInfoFields",False)\
.load(source)\
.write\
.mode("overwrite")\
.saveAsTable(destination)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate sample-level summary statistics and add to the catalog
# MAGIC [![](https://mermaid.ink/img/pako:eNpVkMFKxDAQhl8lZK9d0G5Bm4NQtu1e9OLiqZGSTaZtME1qkipl2Wfx6MWH8xGM7YrKwDAz_zcw8x8xNwIwwa1lQ4du76l242Fp9rsNqnLm2cFK_uQe_0kJqh609BPaBkCZNqhZVrXKvNa8s3FcvzDr5iFarz_f3j_QLmihvkF5_pdzrB8U1M88wKDFnBDV3-H8pGA-o5FKkVV6fXkRZxE3yliyKtPyqkzQL5acsaIo8iKlGke4B9szKcJ_R6oRoth30APFJJQCGjYqTzHVp4COg2AeCiG9sZh4O0KE2ejNftL8p1-YXLLgQo9Jw5QLU5h37hYfZztPXzzldi0?type=png)](https://mermaid.live/edit#pako:eNpVkMFKxDAQhl8lZK9d0G5Bm4NQtu1e9OLiqZGSTaZtME1qkipl2Wfx6MWH8xGM7YrKwDAz_zcw8x8xNwIwwa1lQ4du76l242Fp9rsNqnLm2cFK_uQe_0kJqh609BPaBkCZNqhZVrXKvNa8s3FcvzDr5iFarz_f3j_QLmihvkF5_pdzrB8U1M88wKDFnBDV3-H8pGA-o5FKkVV6fXkRZxE3yliyKtPyqkzQL5acsaIo8iKlGke4B9szKcJ_R6oRoth30APFJJQCGjYqTzHVp4COg2AeCiG9sZh4O0KE2ejNftL8p1-YXLLgQo9Jw5QLU5h37hYfZztPXzzldi0)

# COMMAND ----------

from glow.functions import call_summary_stats, sample_call_summary_stats

source = f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_vars"
destination = f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_sample_qc"

sql(f'select * from {source}')\
.selectExpr("sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles) as qc")\
.selectExpr("explode(qc) as per_sample_qc").selectExpr("expand_struct(per_sample_qc)")\
.write\
.mode("overwrite")\
.saveAsTable(f'{destination}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate variant-level summary statistics and add to the catalog
# MAGIC [![](https://mermaid.ink/img/pako:eNpVkMFOwzAMhl8lyq6dBKUSNAekiqa7jAsTpwZVXpO20dJkSl1QNe1ZOHLh4XgEQsskuFj_b3-2ZZ9o7aSijLYejh3ZPgk7jPvF7DY3pMwBYe91fRhe_pUSUj5bjRN5CIBxbahmWVl3Po4r2cyOrNdf7x-fZGPcW9D3hPOyDbpaqFfwGixWAwL-DFdWzoGEPTgZNe9vtDFsld5dX8VZVDvjPFsVaXFbJH-w5BfjnOc8FZZGtFe-By3DYSdhCREUO9UrQVmQUjUwGhRU2HNAx6MEVFxqdJ4y9KOKKIzodpOtL35hcg3h_J6yBswQsmrueVweOP_x_A2S4nNf?type=png)](https://mermaid.live/edit#pako:eNpVkMFOwzAMhl8lyq6dBKUSNAekiqa7jAsTpwZVXpO20dJkSl1QNe1ZOHLh4XgEQsskuFj_b3-2ZZ9o7aSijLYejh3ZPgk7jPvF7DY3pMwBYe91fRhe_pUSUj5bjRN5CIBxbahmWVl3Po4r2cyOrNdf7x-fZGPcW9D3hPOyDbpaqFfwGixWAwL-DFdWzoGEPTgZNe9vtDFsld5dX8VZVDvjPFsVaXFbJH-w5BfjnOc8FZZGtFe-By3DYSdhCREUO9UrQVmQUjUwGhRU2HNAx6MEVFxqdJ4y9KOKKIzodpOtL35hcg3h_J6yBswQsmrueVweOP_x_A2S4nNf)

# COMMAND ----------

from pyspark.sql.functions import col

source = f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_vars"
destination = f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_variant_stats"

sql(f'select * from {source}')\
.select("*", glow.expand_struct(glow.call_summary_stats("genotypes")))\
.drop("genotypes")\
.selectExpr('*','names[0] as snp_id','nHomozygous[0] as nHom','alleleFrequencies[0] as RefalleleFrequency').drop('names')\
.write\
.mode("overwrite")\
.saveAsTable(f'{destination}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Add Meta Data
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNptUstugzAQ_BXLvSYYkihtOFQqj1SV2kvT9oIj5BgDVoyNsCmNovx7DaRKIvVi7c7OambXe4RUZQz6sGhIXYLXdyx1uxuTzbMHkjh42WIZJB-bLx-UxtTaRyg3teO5rlswqSqmHbbjDqFOu0ffSnh9GWEsDaOl5JQI1Klmz2WBZq43d5fuMtWkqgVLucxVz_wPd8yPscrhqNx13ZVK0RGNSM2RZqShJcpUJ4UimUZ5K4TtYjLD8maUOUgiYsiu4XSvtzelBUg-JTcHEFqCUEU_L5hOH0EQJFeGmooYrmTvaaiGYdL7SOnYlV5L2wcMBsxBsGGRORfCv4vC2I1nF3x-xlcPnjt7mlAlVOPfrVfr-_UCXGiLMy2O4yheYQknsGLWD8_szx2xBABDU7KKYejbMGM5aYXBEMuTpbZ1RgyLM25UA33TtGwCSWvU5iDpXz5yIk7sUiro50Roi7Kh5228kOFQTr92fsCL?type=png)](https://mermaid.live/edit#pako:eNptUstugzAQ_BXLvSYYkihtOFQqj1SV2kvT9oIj5BgDVoyNsCmNovx7DaRKIvVi7c7OambXe4RUZQz6sGhIXYLXdyx1uxuTzbMHkjh42WIZJB-bLx-UxtTaRyg3teO5rlswqSqmHbbjDqFOu0ffSnh9GWEsDaOl5JQI1Klmz2WBZq43d5fuMtWkqgVLucxVz_wPd8yPscrhqNx13ZVK0RGNSM2RZqShJcpUJ4UimUZ5K4TtYjLD8maUOUgiYsiu4XSvtzelBUg-JTcHEFqCUEU_L5hOH0EQJFeGmooYrmTvaaiGYdL7SOnYlV5L2wcMBsxBsGGRORfCv4vC2I1nF3x-xlcPnjt7mlAlVOPfrVfr-_UCXGiLMy2O4yheYQknsGLWD8_szx2xBABDU7KKYejbMGM5aYXBEMuTpbZ1RgyLM25UA33TtGwCSWvU5iDpXz5yIk7sUiro50Roi7Kh5228kOFQTr92fsCL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Sample information
# MAGIC

# COMMAND ----------

import re
import pandas as pd

solac.add_schema(params['phenotype_schema_name'])

source=params['sample_information_url']
destination = f"{params['catalog_name']}.{params['phenotype_schema_name']}.sample_information"
samples_pdf = pd.read_csv(source,sep='\t')

new_col_names = list(map(lambda text:re.sub(r'[^a-zA-Z0-9]', '_',text ).lower().lstrip('_'),samples_pdf.columns))
column_mapping = {current_col_name: new_col_name for current_col_name, new_col_name in zip(samples_pdf.columns, new_col_names)}

spark.createDataFrame(samples_pdf)\
.withColumnsRenamed(column_mapping)\
.write\
.mode("overwrite")\
.saveAsTable(destination)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add GWAS Catalog 

# COMMAND ----------

source = params['gwas_catalog_url']
solac.download_file(source,'./data/gwas_catalog.tsv')

# COMMAND ----------

import pandas as pd
import re

#read the file and rename column names  
solac.add_schema(params['gwas_results_schema_name'])

source = './data/gwas_catalog.tsv'
destination = f"{params['catalog_name']}.{params['gwas_results_schema_name']}.gwas_catalog_full"

gwas_cat_pdf=pd.read_csv(source,sep='\t')

new_col_names = list(map(lambda text:re.sub(r'[^a-zA-Z0-9]', '_',text ).lower().lstrip('_'),gwas_cat_pdf.columns))
column_mapping = {current_col_name: new_col_name for current_col_name, new_col_name in zip(gwas_cat_pdf.columns, new_col_names)}
gwas_cat_pdf['CHR_ID'] = gwas_cat_pdf['CHR_ID'].astype(str)
gwas_cat_pdf['CHR_POS'] = pd.to_numeric(gwas_cat_pdf['CHR_POS'], errors='coerce')
gwas_cat_pdf['SNP_ID_CURRENT'] = gwas_cat_pdf['SNP_ID_CURRENT'].astype(str)
gwas_cat_pdf['RISK ALLELE FREQUENCY'] = gwas_cat_pdf['RISK ALLELE FREQUENCY'].astype(str)

#write the file to the destination table

spark.createDataFrame(gwas_cat_pdf)\
.withColumnsRenamed(column_mapping)\
.write\
.mode("overwrite")\
.saveAsTable(destination)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine sample information and variant data

# COMMAND ----------

variants_table = f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_vars"
sample_info = f"{params['catalog_name']}.{params['phenotype_schema_name']}.sample_information"
destination = f"{params['catalog_name']}.{params['variant_schema_name']}.chr22_sample_vars_pop"

sql(f"""
    with samples_variants as (
        SELECT
            contigName as chr,
            start,
            varId,
            genotypes['sampleId'] as sampleId,
            concat_ws('/', Alleles[genotypes['calls'][0]], Alleles[genotypes['calls'][1]]) as genotype,
            genotypes['calls'][0] + genotypes['calls'][1] as genotype_call
        FROM (
              SELECT contigName, start, end, names[0] as varId, array(referenceAllele, alternateAlleles[0]) as Alleles,explode  (genotypes) as genotypes FROM {variants_table}
        )
    ),
    samples_pop as (
        SELECT
            sample, family_id, population, population_description, gender
        FROM
            {sample_info}
    )
    SELECT
        sv.*, sp.*
    FROM
        samples_variants sv
    JOIN
        samples_pop sp
    ON
        sv.sampleId = sp.sample
"""
).write\
.mode("overwrite")\
.saveAsTable(destination)

# COMMAND ----------

# MAGIC %md
# MAGIC # Check Resulst

# COMMAND ----------

from utils.util import SolAccManager
solac = SolAccManager('./utils/config.yml')
params = solac.config
display(params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## glow_chr22_vars

# COMMAND ----------

source=f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_vars"
sql(f'select * from {source} limit 10').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## glow_chr22_sample_qc

# COMMAND ----------

source=f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_sample_qc"
sql(f'select * from {source} limit 10').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## glow_chr22_variant_stats

# COMMAND ----------

source=f"{params['catalog_name']}.{params['variant_schema_name']}.glow_chr22_variant_stats"
sql(f'select * from {source} limit 10').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## sample_information

# COMMAND ----------

source=f"{params['catalog_name']}.{params['phenotype_schema_name']}.sample_information"
sql(f'select * from {source} limit 10').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## chr22_sample_vars_pop

# COMMAND ----------

source = f"{params['catalog_name']}.{params['variant_schema_name']}.chr22_sample_vars_pop"
sql(f'select * from {source} limit 10').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## gwas_catalog_full

# COMMAND ----------

source=f"{params['catalog_name']}.{params['gwas_results_schema_name']}.gwas_catalog_full"
sql(f'select * from {source} limit 10').display()
