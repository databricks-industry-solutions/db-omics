<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-HLS-red?logo=databricks&style=for-the-badge)](https://www.databricks.com/solutions/industries/healthcare-and-life-sciences)
[![Life Sciences](https://img.shields.io/badge/🔬R&D-Life Sciences-blue?&style=for-the-badge)](hhttps://www.databricks.com/solutions/industries/life-sciences-industry-solutions)
[![POC](https://img.shields.io/badge/🧬GENOMICS- Glow-green?style=for-the-badge)](https://github.com/projectglow/glow)

## Use Case
This solution accelerator is aimed at Computational Biologists working with genomic data. It aims to integrate genomics data with other relevant datasets, presenting findings through interactive dashboards for clinical scientists, geneticists, and other practitioners. Our focus lies in analyzing population-level trends and identifying samples associated with specific causal variants previously discovered through Genome Wide Association Studies (GWAS). We utilize the [GWAS catalog](https://www.ebi.ac.uk/gwas/) in this accelerator.

Primarily, we leverage [Project Glow](https://github.com/projectglow/glow) to access 1000 Genomes Project data stored in public cloud repositories. We compute various sample-level and variant-level summary statistics, constructing a database of human genetic variation alongside GWAS catalog data. Subsequently, we develop an interactive dashboard facilitating exploration of genetic variation across different populations and facilitating the identification of samples associated with specific risk alleles for particular traits or diseases (refer to the demo of the dashboard below).

[![Watch the video](https://img.youtube.com/vi/Hrr_VY_af7g/0.jpg)](https://www.youtube.com/watch?v=Hrr_VY_af7g). 
  
To create the dashboard, you can simply import the lakeview dashboard's JSON file located in `./resources/1000 Genome Samples Dashboard.lvdash.json` in [Lakeview](https://docs.databricks.com/en/dashboards/lakeview.html).

### 1000 Genomes Project Variant Data: 

## Datasets

The [1000 Genomes Project](https://www.internationalgenome.org/) began in 2008 with the aim of mapping human genetic variation. It entailed sequencing the genomes of more than 2,500 individuals from 26 diverse populations worldwide.
The project sought to construct a detailed map of genetic distinctions within human DNA. By analyzing genomes from a broad and varied sample, it identified millions of genetic variants, including single nucleotide polymorphisms (SNPs) and structural variations like insertions, deletions, and copy number variations.

Data from the 1000 Genomes Project serves as a vital tool for researchers investigating human genetics. It has been pivotal in numerous studies exploring the genetic underpinnings of complex diseases, contributing to our understanding of human evolution and population history.

### GWAS Catalog
The GWAS Catalog, a [collaboration between NHGRI and EMBL-EBI](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6323933/), is a curated repository of published genome-wide association studies (GWAS). It houses information on genetic variants, traits, and p-values from these studies, serving as a community resource for data access and analysis tools. The Catalog adheres to FAIR principles, encouraging study identifier citation and offering APIs for data access. Future plans include incorporating unpublished GWAS data. Overall, it is an invaluable asset for researchers exploring the genetic underpinnings of human traits and diseases.

## Reference Architecture
[![](https://mermaid.ink/img/pako:eNp1Ustu2zAQ_JUFc0kBp0hkB611KKAHbRRIL1XrHsiioCUqIiKRAkUlNYJ8S4-55OP6CV1Ril0HyYVYDmdnd5Z7T3JTSBKSayvaCq6-ct312_GSrS-A0fjzT65j9i3bhNCJpq0lKF0aBJMRXP-IMqiU6xCSujgSCIBlc8QjtklWIeSVDYJn2hFxDiwVTmytym8Gof-eFsC-a-V2kCChNteDHJydwd8_j0-wrs0dcK5RHlphO2nx6RNEEfO1fhXl0L3H4pi96N_DScK8Ayu7vvYmokH-oD6Q0nTK5UNrTSPsDjon3qJTym6FVUK7V_gvZ3QJ7ErcyFsl7ya190PF082oAPR3WxuLxlALUtFVWyNs8Q5txRMVrST7ME33IaX70BfFA_zc3Q6nMHxvqeo6PEkTek6DAx68gc8nfPnx4jyIZrnBvsKT1XL1YbWAA20x0SilKV0e8MsjHBPIjDTSNkIVuID36A44cZVsJCchhoUsBX4JJ1w_ILVvC-EkLZQzloTO9nJGRO9MttP5833kpErgcBsSlqLuEJU-58u46H7fH_4B3jD-IQ?type=png)](https://mermaid.live/edit#pako:eNp1Ustu2zAQ_JUFc0kBp0hkB611KKAHbRRIL1XrHsiioCUqIiKRAkUlNYJ8S4-55OP6CV1Ril0HyYVYDmdnd5Z7T3JTSBKSayvaCq6-ct312_GSrS-A0fjzT65j9i3bhNCJpq0lKF0aBJMRXP-IMqiU6xCSujgSCIBlc8QjtklWIeSVDYJn2hFxDiwVTmytym8Gof-eFsC-a-V2kCChNteDHJydwd8_j0-wrs0dcK5RHlphO2nx6RNEEfO1fhXl0L3H4pi96N_DScK8Ayu7vvYmokH-oD6Q0nTK5UNrTSPsDjon3qJTym6FVUK7V_gvZ3QJ7ErcyFsl7ya190PF082oAPR3WxuLxlALUtFVWyNs8Q5txRMVrST7ME33IaX70BfFA_zc3Q6nMHxvqeo6PEkTek6DAx68gc8nfPnx4jyIZrnBvsKT1XL1YbWAA20x0SilKV0e8MsjHBPIjDTSNkIVuID36A44cZVsJCchhoUsBX4JJ1w_ILVvC-EkLZQzloTO9nJGRO9MttP5833kpErgcBsSlqLuEJU-58u46H7fH_4B3jD-IQ)

## Authors
<amir.kermany@databricks.com>

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 

## License

&copy; 2024 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
  Project Glow                           | an open-source toolkit to enable bioinformatics at biobank-scale and beyond.|[Apache 2.0](https://github.com/projectglow/glow/blob/main/LICENSE.txt) | https://github.com/projectglow/glow