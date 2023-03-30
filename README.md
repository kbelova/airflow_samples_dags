# airflow samples dags
Small collection of ETL pipelines for Apache Airflow to demonstrate it's power and corner cases.

# how to run
## Pre-installed Software required
* docker
* docker-compose
 
Run has been tested with Windows 11 and IntelliJ Idea Community.
Should be working simillary on MacOS and Linux.

## For the first run:
1. open terminal in idea and make sure you are in the project root directory: `cd ~/<projects>/airflow_samples_dag` 
2. `docker-compose up airflow-init`
3. `docker-compose up` to see logs in command line, or add `-d` to run in deamon mode.

It will create 2 additional folders in your project: /logs and /plugins. 
* /logs is ignored by git, there all the logs of airflow runs are placed;
* /plugins - here custom plugins can be added.

Docker here setup in the way that any changes to files in plugins or /dags folder are picked up by airflow. 
Though after changes to custom plugin airflow restart might be required.

To get access to Airlfow WebUI: localhost:8080, 
user: airflow
pass: airflow

## Usefull commands
`docker-compose down` or ctrl+C in terminal where airflow is running to stop containers.
`docker ps` list of docker container up and running
`docker ps -a` same, but also shows stopped containers
