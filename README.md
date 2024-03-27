# air-quality-project

- [Welcome](#welcome)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Creating Visualisations](#creating-visualisations)
- [Running the Code](#running-the-code)


# Welcome
This project contains an end-to-end data pipeline, written in Python. It is my final project for [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp#data-engineering-zoomcamp) in the 2024 Cohort. 

The application reads from [Open-Meteo](https://open-meteo.com/) two APIs. One is [Air Quality API](https://open-meteo.com/en/docs/air-quality-api) and second is [Weather Forecast API](https://open-meteo.com/en/docs), transforms it and uploads it to Google Cloud Storage. Then it is loaded from GCS into BigQuery and created a few tables, all orchestrated in [Mage](https://docs.mage.ai/introduction/overview).

## Prerequisites
1. [Docker](https://docs.docker.com/engine/install/)
2. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
3. [Terraform](https://developer.hashicorp.com/terraform/install)

# Setup
Before running the code you need to follow the steps below.

## Setting up GCP
Google Cloud is a suite of Cloud Computing services offered by Google that provides various services like compute, storage, networking, and many more. It is organized into Regions and Zones.

Setting up GCP would require a GCP account. A GCP account can be created for free on trial but would still require a credit card to signup.

1. Start by creating a GCP account at [this link](https://cloud.google.com/)
2. Navigate to the GCP Console and create a new project. Give the project an appropriate name and take note of the project ID.
3. Create a service account.

   - In the left sidebar, click on "IAM & Admin" and then click on "Service accounts."

   - Click the "Create service account" button at the top of the page.

   - Enter a name for your service account and a description (optional).

   - Select the roles you want to grant to the service account. For this project, select the BigQuery Admin, Storage Admin and Compute Admin Roles.

   - Click "Create" to create the service account.

   - After you've created the service account, you need to download its private key file. This key file will be used to authenticate requests to GCP services.

   - Click on the service account you just created to view its details.

   - Click on the "Keys" tab and then click the "Add Key" button.

   - Select the "JSON" key type and click "Create" to download the private key file. This key would be used to interact to the google API from Mage.

   - Store the json key as you please, but then copy it into the mage directory of this project  and
give it exactly the name `my-airquality-credentials.json`.

4. This application communicates with several APIs. Make sure you have enabled the BigQuery API.
- Go to [BigQuery API](https://console.cloud.google.com/apis/library/browse?hl=sv&project=air-quality-project-417718&q=bigquery%20api) and enable it.


## Running the Code
*Note: these instructions are used for macOS/Linux/WSL, for Windows it may differ*

1. Clone this repository
2. `cd` into the mage directory
3. Rename `dev.env` to simply `.env`.

4. Now, let's build the container

```bash
docker compose build
```

5. Finally, start the Docker container:

```bash
docker compose up
```

6. We just initialized a mage repository. It is present in your project under the name `air-quality`. Now, navigate to http://localhost:6789 in your browser!

7. It's time to create google cloud resorces. For that, we are using **terraform**. My resources are created for region **EU**. If needed, you can change it in **varibales.tf** file. In this file you need to change the project ID to the project ID you created.

   `cd` into the terraform directory and to prepare your working directory for other commands we are using:

```bash
terraform init
```
8. To create or update infrastructure we are using:

```bash
terraform apply
```
9. To destroy previously-created infrastructure we are using:

```bash
terraform destroy
```
 **IMPORTANT**: This line uses when you are done with the whole project. 

10. Time to work with mage. Go to the browser and in the folder **pipeline** you will find pipeline named **air_quality_api**. Run it. After it finished running the all blocks in a google bucket you should have two CSV files and in the BigQuery you should have all tables.

## Creating Visualisations

With your google account, log in at [Google looker studio](https://lookerstudio.google.com/navigation/reporting)

Connect your dataset using the Big Query Connector

- Select your project name then select the dataset. This would bring you to the dashboard page

Create your visualizations and share.

As a general observation, most aeropalynology studies indicate that temperature and wind have a positive correlation with airborne pollen concentrations, while rainfall and humidity are negatively correlated.