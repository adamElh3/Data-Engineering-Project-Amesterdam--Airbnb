# Data-Engineering-project-Amesterdam-Airbnb

# Project Description: Batch Processing with Amsterdam Airbnb Dataset

In this data engineering project, we leverage the rich Amsterdam Airbnb dataset to perform insightful batch processing and generate valuable analytics for our dashboard. The dataset provides a comprehensive view of Airbnb listings and user interactions in Amsterdam, offering a wealth of information to extract meaningful insights.

# Objective:
The primary objective is to analyze the Amsterdam Airbnb dataset through daily batch processing, applying transformations that result in the creation of structured tables for our analytics dashboard. By delving into this dataset, we aim to uncover trends, patterns, and metrics related to the city's Airbnb landscape.

# Dataset: Amsterdam Airbnb
The Amsterdam Airbnb dataset captures a diverse range of information, including property details, pricing, availability, and user reviews. This dataset is a valuable resource for understanding the dynamics of the city's short-term rental market, providing insights into property popularity, pricing strategies, and customer preferences.

# Batch Processing Workflow:

Data Extraction:

Extract relevant information from the Amsterdam Airbnb dataset, focusing on key attributes such as property types, neighborhood details, pricing, and user reviews.
Data Transformation:

Apply data transformations to enhance the dataset, handling missing values, standardizing formats, and deriving additional features for more comprehensive analysis.
Aggregations and Metrics:

Aggregate data to generate meaningful metrics, including average pricing per neighborhood, popular property types, 
seasonal trends, and overall occupancy rates.

Table Creation:

Create structured tables optimized for efficient querying, supporting the analytics dashboard requirements.


# Architecture

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/Architecture.jpg)

# Technologies

1. **Data Extraction and Transformation using Apache Spark:**
   - Spark jobs are used to extract and transform the Amsterdam Airbnb dataset.

2. **Loading Data to Amazon S3:**
   - Processed data is stored in a structured format on Amazon S3.

3. **Ingestion into Amazon Redshift:**
   - Data is ingested from S3 into Amazon Redshift, providing a scalable and analytical data warehouse.

4. **Utilizing dbt (Data Build Tool):**
   - dbt models are implemented for additional transformations and data modeling.

5. **Integration with Power BI:**
   - Power BI connects to Amazon Redshift to create interactive dashboards and reports.
     
6. **Docker for Containerization:**

  - Docker containers encapsulate the entire Spark job execution environment, ensuring consistency across different environments.
  - Containerization enhances portability and reproducibility of the Spark jobs.
    
7. **Terraform for Infrastructure Provisioning:**

  - Terraform is used to define and provision infrastructure components required for Spark job execution.
  - Infrastructure provisioning includes virtual machines, networks, and storage resources.
      
8. **Apache Airflow for Workflow Orchestration:**

  - Apache Airflow is employed to orchestrate the entire batch processing workflow.
  - Define and schedule tasks (daily) in Airflow to automate the execution of Spark jobs, data loading, and other steps in the pipeline.   

# Project Structure
-  airflow/: Apache Airflow DAGs and configurations for workflow orchestration.
-  terraform/: Terraform configurations for infrastructure provisioning.
-  spark/: Contains Spark jobs for data extraction and transformation.
-  dbt_models/: dbt models for further transformations and modeling.

# Getting Started


   ## 1. Docker Usage:
   Docker serves as our central tool for orchestrating and managing diverse services throughout this project.

   Installation Instructions:
   Begin by visiting the official Docker website and download Docker Desktop suitable for your operating system.

   Verification Process:
   After completing the installation, open a terminal or command prompt. Confirm the successful installation by executing the command docker --version. This        command will display the Docker version installed on your system, ensuring that the installation was completed successfully.


   ## 2. Main
   Clone the repository:
   
       git clone https://github.com/adamElh3/Data-Engineering-Project-Amesterdam--Airbnb.git

   Navigate to the project directory:

       cd Data-Engineering-Project-Amesterdam--Airbnb   
   

   ## 3. Terraform Implementation:
   To streamline the provisioning of infrastructure for our project, we can leverage a dedicated Terraform repository available at             
   <a href="https://github.com/KopiCloud/terraform-aws-redshift-serverless" title="About Me">Terraform-aws-redshift-serverless.</a>.

   We use Terraform to create a S3 bucket and Redshift.

   Navigate to the terraform folder and complete credentials.

   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/terraform_complete0.png)
   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/terraform_complete1.png)

   

   ### a.Note: 
   Ensure that you have the necessary AWS credentials configured on your local machine for Terraform to interact    
   with your AWS account.

   ### b.Initialize Terraform:
   Initialize Terraform within the directory to download necessary plugins and modules:


      terraform init

   ### c.View Execution Plan:
   Optionally, generate and view an execution plan to understand the changes Terraform will make to your 
   infrastructure:
   
      terraform plan
   ### d.Apply Terraform Changes:
   Apply the Terraform changes to create or update the infrastructure:

      terraform apply
   Confirm the changes by typing 'yes' when prompted.

   ### e.Results:
   work_group,namespace,redshift_database and s3 bucket are added.
   
   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/aws_bucket.png)
   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/namespace_workgroup.png)
   
   
   
   


# Building the Data Pipeline: A Guided Approach
   
# S3_To_Redshift:
You have to create a table named listings in redshift using your own schema which you create in the python script.
After executing the Spark script using airflow, the data is loaded into Amazon S3 and then further imported into Redshift using the COPY command.


![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/insert_schema.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/listings_to_aws.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/s3_to_redsh.png)

# Setting up an account in dbt Cloud and initializing a new project involves the following steps:
## Create repository on Github:
We simply create an empty repo on Github and click on Git Clone to copy the SSH key with which we will link the dbt project . The connection with Github is made in two parts, for now we are only interested in the key, later we will see how to configure the deploy key generated from dbt .

To add new ssh-key to your github repository : you can refer to the documentation:
<a href="https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account" title="About Me">Connecting-to-github-with-ssh</a>


![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/add_ssh_key.png)

After generating your ssh-key:

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/git_clone_dbt2.png)


## Fill credentials:
we already set up redshift_database using terraform.

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/redshift_conn_dbt.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/complete_dbt.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/complete_dbt2.png)


## Cloning:
Now it is our turn to configure the Github repository that we have previously created and carry out the second step that we had pending. We select Git Clone     and paste the SSH Key that we copied before. We press the Import button .

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/git_clone_dbt.png)

It will generate a deployment key that we need to copy into the Github repository configuration.

## Deploy dbt_key:
Returning to our Github repository, click on Settings and in the Security section click on Deploy Keys to add it. **It is necessary to check the Allow write access option:**

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/deploy_dbt_key.png)


If we click the Next button in the dbt project configuration we will have finished.

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/project_ready_dbt.png)

## Initialize project:
We access Develop and we must initialize our project in dbt cloud:

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/initialize_dbt.png)

 ### Note : Remember that since it is linked to a repo on github, if you want to work on the dbt cloud GUI you need to first create a branch. To execute any command from GUI, we can use the console that we have in the foot:

To learn how to create profile files, schema files, and SQL files, please refer to the documentation available on the dbt website.

## Some dbt sql models dags:

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dag_dbt1.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dag_dbt2.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dag_dbt3.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dag_dbt4.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dag_dbt5.png)



## Running models using dbt run command:

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dbt_run.png)


## Tables and views in Redshift serverless:
After a successful run of models ,if we check redshift , new tables and views are generated .

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/new_tables_dbt1.png)



## Create jobs in dbt:
Dbt includes a scheduler where you can configure jobs to run the models in production. Jobs can be scheduled or executed manually and each one can have one or more commands (build several models, launch tests, etc.).

Before creating the job, the first thing we would have to do is configure a production environment in order to have the development environment differentiated from the deployment environment. To create it, we go to Deploy > Environments and click on the Create Environment button . In Deployment credentials we must indicate the destination redshift dataset. We can pre-create it from aws or type a name and dbt will automatically create it:

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/PRODUCTION_ENV.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/create_job1.png)

Now we could create a job to publish the development changes to production. To do this, we are going to click on Deploy > Jobs and the Create Job button . Let's specify a name:

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/create_job2.png)

We mark that it generates the documentation and we add the command:dbt build(to build them).

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/create_job3.png)

Finally, we configure the programming in the triggers section:

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/create_job4.png)

click Run now!

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/create_job5.png)

when time arrive , the job will be trrigered.

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/create_job6.png)

Successful job !

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/job_success_dbt.png)

Tables in production:

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/new_tables_dbt.png)


## Documentation:
Once the job has been executed, we check that all the steps have completed correctly and the documentation has been generated:
You can use this command to generate documents.

         dbt docs generate

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/doc_command.png)

if you want to see your documentations:
Navigate to the "Docs" Tab:

In the dbt Cloud workspace, find and click on the "Docs" tab.
Explore the Documentation:

The "Docs" tab will provide a navigation menu on the left, allowing you to explore documentation related to models, tests, and other elements in your dbt project.
You can click on specific models or other artifacts to see detailed information, descriptions, and lineage diagrams.

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/docum1.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/docum2.png)

## Create a Pull Request in Your Version Control System:
Navigate to the version control system's interface.
Open a pull request for the branch you just pushed.
Provide a title and description for your pull request.
Submit the pull request.

From Github we merge branches.

![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/merge0.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/merge1.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/merge2.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/merge3.png)
![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/merge4.png)



# Airflow process:

## 1. Creating network and Run docker file alongside docker-compose file 

      docker network create my_network
      docker-compose up -d --build
      

   ## 2. Configure Airflow User

   Effortlessly create an Airflow user endowed with admin privileges:


      docker-compose run airflow_webserver airflow users create --role Admin --username admin --email admin -- 
      firstname admin --lastname admin --password admin

   
   ## 3. Access to the Airflow container,  
         
         docker container ps
         docker exec -it <your_airflow_container_id> /bin/bash
   
   ## 4. Validate DAGs

   Ensure the integrity of your Directed Acyclic Graphs (DAGs) with the following command:

         airflow dags list
   
   ## 5. Start Airflow Scheduler

   Initiate the DAG by running the scheduler:

         airflow scheduler

   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dag0.png)
   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/final_dag.png)
   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/final_dag1.png)

         










   

   
         
         

# Setting up a connection between Apache Airflow and dbt within the Airflow Admin interface

   ## a.Access Airflow Admin Interface:
   Navigate to the Airflow Admin interface by visiting the designated URL in your web browser. Typically, this is accessible at http://localhost:8080.

   ## b.Go to the "Connections" Page:
   Once inside the Admin interface, locate and click on the "Connections" option. This section allows you to   
   manage and configure various connections, including the one to dbt.

   ## c.Create a New Connection:
   Within the "Connections" page, identify the option to create a new connection. Click on this option to       
   initiate the setup process.

   ## d.Fill in Connection Details:
   Provide the necessary details to establish the dbt connection. This includes specifying the connection ID,    
   which is a unique identifier for this connection. For clarity, name it appropriately, such as       
   "dbt_connection."

   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dbt_airflow0.png)

   ## e.Choose Connection Type:
   Select the connection type relevant to dbt. This may involve choosing a database type, specifying the host,port, and credentials required to access the dbt instance.

   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dbt_airflow1.png)

   ## f.Save the Connection:
   Go to dbt cloud ,You can find your User API token in the Profile page under the API Access label,Once all the connection details are accurately filled in, save the connection configuration. This action finalizes the setup and ensures that Airflow can communicate with dbt seamlessly.

   ![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/dbt_airflow2.png)

   ## j.Verify Connection:
   After saving the connection, it's crucial to perform a verification step. Test the connection from within the Airflow Admin interface to confirm that Airflow    can successfully connect to dbt using the provided          
   configuration.

# Setting up a connection between Apache Airflow and Apache Spark within the Airflow Admin interface:
   Refer to this repository(Thanks to him)
   <a href="https://github.com/cordon-thiago/airflow-spark" title="About Me">Airflow_Spark</a>



# Visualisation:


![alt text](https://github.com/adamElh3/Data-Engineering-project-Amesterdam-Airbnb/blob/main/images_github/report.jpg)
   
   



# Analyzing Some Project Files
   ## 1.Docker_compose:
   ### a. PostgreSQL Database for Apache Airflow (airflow_db):

   Utilizes the PostgreSQL image (version 16.0).
   Configures environment variables for the PostgreSQL user, password, and database.
   Logging options are set to manage log files.
   Connected to the custom network named my_network.
   ### b. Apache Airflow Webserver (airflow_webserver):
   
   Builds the Apache Airflow image with the specified Dockerfile (.).
   Initializes Airflow database (airflow db init) and creates an admin user during container startup.
   Connects to the PostgreSQL database defined in airflow_db.
   Maps local directories for DAGs, Spark scripts, and resources.
   Exposes Airflow webserver on port 8082.
   Implements a health check to monitor the availability of Airflow.
   ### c. Apache Spark Master and Workers (spark_master, spark-worker-1, spark-worker-2, spark-worker-3):
   
   Utilizes the Bitnami Spark image (version 3).
   spark_master serves as the Spark cluster master node, exposing Spark UI on port 8085.
   spark-worker-1, spark-worker-2, and spark-worker-3 are Spark worker nodes connected to the master.
   Configures Spark properties and environment variables for the Spark cluster.
   Maps local directories for Spark scripts and resources.
   ### d. Custom Network (my_network):
   
   Defines a custom bridge network named my_network for communication between containers.
   ### e. Volumes:
   
   Defines volumes (spark and dags) for persistence and sharing data between the host and containers.

 ## 2.DAG Definition:

   The DAG is named 'spark_and_dbt_dag' and is configured with default parameters.
   It has a start date of January 11, 2024, and does not depend on past executions.
   Email notifications on failure or retry are disabled.
   It allows one retry with a delay of 5 minutes.
   ### a.Schedule Interval:
   
   The DAG is scheduled to run daily at midnight ('0 0 * * *'), and it does not catch up on missed runs.
   ### b.Tasks:
   
   1-install_requirements_task:
   
   Task ID: 'install_requirements'
   Executes a Bash command to install Python dependencies specified in '/opt/airflow/spark/app/requirement.txt'.
   
   2-spark_task:
      - Task ID: 'run_spark_task'
      - Executes a Spark job using SparkSubmitOperator.
      - The Spark job runs the script '/opt/airflow/spark/app/spark_airbnb_amesterdam.py'.
   
   3-dbt_task:
   
   Task ID: 'run_dbt_command'
   Executes a dbt Cloud job using DbtCloudRunJobOperator.
   The operator is configured with the connection ID 'dbt_conn' and a specific job ID ('yours').

## Conversion:Jupyter to .py
   During my project work, I initially utilized Jupyter locally as my primary development environment
   To facilitate reproducibility and deployment, I employed the Jupyter command-line tool to convert my Jupyter notebook (.ipynb) files into executable Python      script (.py) files using this command :   

            jupyter nbconvert --to script spark_airbnb_amesterdam.ipynb



# Special mention

I would like to express my heartfelt gratitude to <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp" title="About Me">DataTalks.Club</a> for their invaluable contribution to my journey in learning data engineering. The course offered by the club have been instrumental in shaping my skills and knowledge in this field.

A huge shout-out to them for offering exceptional data engineering course – for free!. If you're eager to dive into this exciting field, check out their course. Don't miss out on the opportunity to enhance your skills and join their community!.

   
   

