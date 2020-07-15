/***** Structure of Directories and Files *****/
project
	- app_code
		- project: Used for building the Spark application for the analytic phase.
		- src
			- main
				- scala
					- analytic.scala: ScalaSpark source code for the analytic phase.
		- target: Used for building the Spark application for the analytic phase.
		- analytic.sbt: Used for building the Spark application for the analytic phase.
	- data_ingest
		- data_ingest.txt: Contains commands typed on NYU Dumbo for the data ingest phase.
	- etl_code
		- project: Used for building the Spark application for the ETL phase.
		- src
			- main
				- scala
					- clean.scala: ScalaSpark source code for the ETL phase.
		- target: Used for building the Spark application for the ETL phase.
		- clean.sbt: Used for building the Spark application for the ETL phase.
	- profiling_code
		- project: Used for building the Spark application for the profiling phase.
		- src
			- main
				- scala
					- profile.scala: ScalaSpark source code for the profiling phase.
		- target: Used for building the Spark application for the profiling phase.
		- profile.sbt: Used for building the Spark application for the profiling phase.
	- screenshots
		- analytic: Contains screentshots for a successful run of the Spark application for the analytic phase.
		- data_sample: Contains screenshots showing data samples for each dataset.
		- etl: Contains screenshots for a succesfful run of the Spark application for the ETL phase.
		- profiling: Contains screenshots for a successful run of the Spark application for the profiling phase.
		- dashboard.PNG: A screenshot of the Tableau dashboard for the visualization phase.
	- test_code: Contains some test code and file not used in the project.
	- dashboard.twb: The Tableau workbook that contains the dashboard and various plots for the visualization phase.
	- Readme.txt: This file

/***** Data Ingest Stage *****/
*** On Dumbo ***
cd ~/BDAD/project
curl -o NYC_Jobs.csv https://data.cityofnewyork.us/api/views/kpav-sd4t/rows.csv?accessType=DOWNLOAD&bom=true&format=true
curl -o NYC_Payroll.csv https://data.cityofnewyork.us/api/views/k397-673e/rows.csv?accessType=DOWNLOAD&bom=true&format=true
curl -o Chicago_Payroll.csv https://data.cityofchicago.org/api/views/xzkq-xp2w/rows.csv?accessType=DOWNLOAD&bom=true&format=true
hdfs dfs -mkdir -p BDAD/data
hdfs dfs -put *.csv BDAD/data

* The three raw datasets can be found at /home/jl11046/BDAD/data

/***** Setup Environment *****/
*** On Dumbo ***
module load spark/2.4.0
module load git/1.8.3.1
module load sbt/1.2.8
module load scala/2.11.8

/***** ETL Stage *****/
*** On Dumbo ***
Input data locations: 
hdfs://dumbo/user/jl11046/BDAD/data/Chicago_Payroll.csv
hdfs://dumbo/user/jl11046/BDAD/data/NYC_Payroll.csv
hdfs://dumbo/user/jl11046/BDAD/data/NYC_Jobs.csv

Output data locations: 
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/chicago
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc_history
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc_jobs

Commands to run the ETL code:
cd ~/BDAD/project/etl_code
sbt package
spark2-submit --name clean --class clean --master yarn --deploy-mode cluster --verbose target/scala-2.11/clean_2.11-0.1.0-SNAPSHOT.jar

/* Data Schema after cleaning */
Dataset 1: Chicago_Payroll
Column_name     Type    	Description
Department      String  	Department the employee works for.
Title           String  	Title of the employee.
Salary          Integer 	Annual salary of the employee.

Dataset 2: NYC_Payroll
Column_name     Type    	Description
Department      String  	Department the employee works for.
Title           String  	Title of the employee.
Salary          Integer 	Annual salary of the employee.

Dataset 3: NYC_Jobs
Column_name     Type    	Description
Year            Date    	Posting year of the job opening.
Department      String  	Department the employee works for.
Title           String  	Title of the employee.
Salary          Integer 	Annual salary of the employee.

Dataset 4: NYC_Payroll_with_year
Column_name     Type    	Description
Year            Date    	The year of the payroll record.
Department      String  	Department the employee works for.
Title           String  	Title of the employee.
Salary          Integer 	Annual salary of the employee.

/***** Profiling Stage *****/
*** On Dumbo ***
Input data locations:
hdfs://dumbo/user/jl11046/BDAD/data/Chicago_Payroll.csv
hdfs://dumbo/user/jl11046/BDAD/data/NYC_Payroll.csv
hdfs://dumbo/user/jl11046/BDAD/data/NYC_Jobs.csv 
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/chicago
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc_history
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc_jobs

Output data location:
hdfs://dumbo/user/jl11046/BDAD/data/profiling

Commands to run the profiling code:
cd ~/BDAD/project/profiling_code
sbt package
spark2-submit --name profile --class profile --master yarn --deploy-mode cluster --verbose target/scala-2.11/profile_2.11-0.1.0-SNAPSHOT.jar

/* Information gathered after profiling */
1. Number of records
There were 33702 records in the original Chicago Payroll dataset. 25527 records remaining after cleaning.
There were 3333080 records in the original NYC Payroll dataset. 1724584 records remaining after cleaning, including 299853 records for Year 2019.
There were 2552 records in the original NYC Jobs dataset, 2084 records remaining after cleaning.

2. Number of departments and titles
There are 36 city departments with 978 titles in Chicago in 2019.
There are 148 city departments with 1264 titles in NYC in 2019.
There are 156 city departments wth 1529 titles in NYC from 2014 to 2019.
There are 51 city departments with 899 titles in NYC hiring in 2019.

3. Maximum and minimum salary values
The maximum annual salary in Chicago in year 2019 is 275004, the minimum is 14160.
The maximum annual salary in NYC in year 2019 is 352763, the minimum is 22500.
The maximum annual salary in NYC from year 2014 to 2019 is 352763, the minimum is 1.
The maximum average annual salary proposed in NYC city job postings in year 2019 is 210000, the minimum is 29561.

/***** Anlytics Stage *****/
*** On DUMBO ***
Input data locations:
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/chicago
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc

Output data locations:
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/chicago_departments_cleaned
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc_departments_cleaned
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/combined
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/chicago_police_officer
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/chicago_police_officer_stats
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc_police_officer
hdfs://dumbo/user/jl11046/BDAD/data/cleaned/nyc_police_officer_stats

Commands to run the application code:
cd ~/BDAD/project/app_code
sbt package
spark2-submit --name analytic --class analytic --master yarn --deploy-mode cluster --verbose target/scala-2.11/analytic_2.11-0.1.0-SNAPSHOT.jar

Analytic 1: Comparing Chicago and NYC city employee salaries in 2019 at the department level.
Dataset used: Chicago_Payroll, NYC_Payroll
Step 1: Further clean the department field in each dataset. Select departments present in both datasets only, with possible renaming.
Step 2: Union the two cleaned dataset, adding a new column "City". (for Tableau visualization)

Analytic 2: Comparing Chicago and NYC police officer salaries in 2019.
Dataset used: Chicago_Payroll, NYC_Payroll
Step 1: Select records for the police department, police officer only.
Step 2: Union the two cleaned dataset, adding a new column "City". (for Tableau visualization)

Findings: 
In 2019, Chicago has 22705 full time city employees, 13824 in the police department, 11428 police officers; 
	 NYC has 104525 full time city employees, 50700 in the police department, 24676 police officers.
	  
In 2019, Chicago has 11428 police officers, and their average salary is $85095.0, with a standard deviation of 11768.5.
	 NYC has 24676 police officers, and their average salary is $73060.9, with a standard deviation of 19139.0.

Analytic 3: Observing the trend in employee salaries from year 2014 to 2019, and the trend in new city employee salaries from 2016 to 2020 in NYC.
Dataset used: NYC_Payroll_with_year, NYC_Jobs
Use Tableau for visualization.

/***** Visualization Stage ******/
Commands to access the visualization:
Connect to NYU VPN.
Enable ssh port forwarding: ssh -L 4483:babar.es.its.nyu.edu:10001 jl11046@dumbo.hpc.nyu.edu
Open the Tableau workbook file dashboard.twb in the project root directory.
Log in Spark SQL.