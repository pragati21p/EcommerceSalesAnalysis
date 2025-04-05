## Task
E-commerce Sales Data Processing with Databricks

### Prerequisites
Log into Azure DevOps and Databricks Workspace.

### File Structure

This repository contains
  1.  the Python code to run, _(path: workspace/)_
  2.  the JSON specification of the Spark-cluster which will run the code, _(path: pipelines/)_
  3.  shell build scripts which are executed on the build server, _(path: pipelines/)_
  4.  the files to upload for analysis _(path: files/)_
  5.  the unit test file _(path: tests/)_
  5.  the YAML configuration of the two pipelines which 
      - checks code coverage (5) followed by copy the code (1), 
      - create the runtime (2) and 
      - upload the files (4) 
      by executing the build scripts (3). _(path: pipelines/)_
  6.  output csv files, storing data from sql queries

```md
Ecommerce Sales Data Analysis
├── files
│   ├── Customer.xlsx
│   ├── Orders.json
│   ├── Products.csv
├── outputs
│   ├── statsByCustomer.csv
│   ├── statsByCustomerYear.csv
│   ├── statsByYear.csv
│   ├── statsByYearCategory.csv
├── pipelines
│   ├── build-cluster.yml
│   ├── build-workspace.yml
│   ├── config.cluster.json
│   ├── databricks-cli-config.sh
│   ├── databricks-cluster-create.sh
│   ├── databricks-cluster-delete.sh
│   ├── databricks-library-install.sh
│   ├── databricks-workspace-import.sh
├── tests
│   ├── test_EcommerceAnalysis.py
├── workspace
│   ├── EcommerceAnalysis.py
├── requirements.txt
```

### CI/CD

In order to deploy the project, 2 yml files need to be run:
- build-cluster.yml (in order to create cluster with desired config/libraries)
- build-workspace.yml (in order to import workspace with required files, py files)

```md
cicd pipeline
├── build_cluster.yml
│   ├── databricks-cli-config.sh
│   ├── databricks-cluster-delete.sh
│   ├── databricks-cluster-create.sh
|   |   ├── config.cluster.json
│   ├── databricks-library-install.sh
├── build-workspace.yml
│   ├── _run pytest_
│   ├── databricks-cli-config.sh
│   ├── _upload files_
│   ├── databricks-workspace-import.sh
```

### Workflow

Source file _EcommerceAnalysis.py_ does follows:
- Creates raw tables for Customer, Orders, Products
- Creates enriched table for customers and products 
- Create an enriched table which has
  1. order information 
  2. Profit rounded to 2 decimal places
  3. Customer name and country
  4. Product category and sub category
- Create an aggregate table that shows profit by 
  1. Year
  2. Product Category
  3. Product Sub Category
  4. Customer
- Using SQL output the following aggregates
  1. Profit by Year
  2. Profit by Year + Product Category
  3. Profit by Customer
  4. Profit by Customer + Year


### Outputs

4 stats to be queried:
- Using SQL output the following aggregates
    1. Profit by Year
    2. Profit by Year + Product Category
    3. Profit by Customer
    4. Profit by Customer + Year


### Future Enhancements

- Code structure can be improved and separate _utils_ package can be created. I have kept the code in one file for its readability and avoiding reference lookup.
- Data cleaning can be more effective with clear requirements like handling null customer name, category and sub category (should be 'NaN' or removed from detailed table)
- Logs can be added in script