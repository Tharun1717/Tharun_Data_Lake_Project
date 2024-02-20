
Americold Data Lake Orion Project Overview:

The Americold Data Lake Orion project is a cutting-edge initiative aimed at constructing a
robust data lake infrastructure on the Amazon Web Services (AWS) platform. This endeavor
harnesses a spectrum of AWS services including S3, EC2, AWS Glue, Step Functions, EventBridge,
Secrets Manager, Lambda, Redshift, DocumentDB, and Athena. The primary objective of this data
lake is to effectively process and manage data originating from Americold's extensive network of
warehouse management sites.

Data Lake Architecture:
The data lake architecture is meticulously designed and comprises four key stages, each serving a
critical function in the data processing pipeline:

Bronze Layer:
In this initial stage, the i3pl team is responsible for the pivotal task of loading XML
data generated from the Warehouse Management System (WMS) into the designated S3 bucket
named 'i3pl-xml-bronze.' The data is systematically organized within the 'processed' folder,
marking the inception of the data processing journey.

Silver Layer:
At this stage, an EC2 instance aptly christened 'Jetparser' assumes a central role.
It processes the incoming XML files with precision, effecting a transformation into the
widely compatible JSON format. The resulting JSON files are meticulously categorized and
deposited in the 'i3pl-json-silver' S3 bucket. This ensures an organized and accessible
repository for subsequent stages of data processing.

Staging Layer:
The JSON files, now residing in the Silver Layer, are poised for further advancement.
Through the utilization of AWS Glue, tailored jobs are executed, enabling the seamless
transfer of data to Redshift staging tables. These Glue jobs are meticulously designed
to accommodate specific file types, ensuring a structured and optimized approach to data storage.

Gold Layer:
The pinnacle of data processing is achieved in the Gold Layer. Data residing within the
staging tables undergoes a meticulous Inventory Consolidation (IC) process. Here, critical
business logic is applied to the dataset, ultimately facilitating its integration into distinct
Inventory Consolidate Gold Tables. This stage represents a critical juncture, ensuring accurate
and meaningful representation of inventory data.

Cust KPI Process:
The project incorporates a specialized process designed to generate comprehensive Customer
Key Performance Indicator (KPI) reports. This process is underpinned by a sophisticated
framework comprising five staging tables, namely: cust_kpi_sys, cust_kpi_man, cust_kpi_cwms,
cust_kpi_lease, and cust_kpi_aux. Through the orchestration of Glue jobs and stored procedures,
these tables collectively culminate in the creation of a detailed report structure, instrumental
for Flash reports and analytical insights.

Post-Processing:
Following the successful compilation of data in the Gold Layer, a seamless mechanism is
established to facilitate the transfer of Redshift tables to an external organization.
This is achieved by configuring Virtual Private Cloud (VPC) settings and providing requisite
connection details. The receiving organization, referred to as 'Recview,' subsequently
accesses and consumes the data, marking the downstream utilization of the meticulously processed dataset.

Conclusion:
The Americold Data Lake Orion project embodies a state-of-the-art data processing paradigm,
leveraging the extensive capabilities of AWS. The project attests to a profound grasp of data
engineering principles, underscored by a seamless integration of diverse AWS services.
The architecture is engineered for scalability, ensuring real-time data processing and
subsequent downstream consumption by external stakeholders. This endeavor stands as a
testament to the fusion of cutting-edge technology with pragmatic business application,
exemplifying excellence in data management and analytics.




