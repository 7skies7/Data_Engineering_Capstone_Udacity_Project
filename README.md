# Soccer Leagues Points
### Data Engineering Capstone Project

#### Project Summary
European Soccer Database consist of all the matches players from Season 2008 to 2016. Dataset has many different tables we will have to understand and gather meaningful insights and transform the data to fetch the league points table at every stage of the respective season for the leagues.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 
The scope of the Project is to build a Data Pipeline using Apache Airflow to build pipeline of tasks which include create tables, load the staging tables to Amazon Redshift, transfrom the staging tables using Pandas and load the transformed data back to Amazon Redshift.
Raw match data consist of all the +25000 matches played from the season 2008 to 2016 in all the 11 European Countries, we need to calculate the points earned by teams in a single match for each stage. 
For each match played, a team with the more no of goals than the other will earn 3 points, if it is a draw then it will 1 point else if team losses then it 0 point.
A stage represents matches played in a single week of season by all the teams in a single league. So will try to transform data so that we can check at which week(stage) of the season how much points were earned by the team. This data will then be used to analyse the performance of the team.



#### Describe and Gather Data 

The dataset used in this project comes from the Kaggle's European Soccer Database as you can see below image which lists all the tables. But it is sqllite database, so for convenience I converted the dataset to csv of respective table and uploaded it to S3 as shown in the second image.
![assets/dataset](assets/dataset.png)
![assets/dataset_s3](assets/dataset_s3.png)

This dataset includes the following:
1. +25,000 matches
2. +10,000 players
3. 11 European Countries with their lead championship
4. Seasons 2008 to 2016
5. Players and Teams' attributes* sourced from EA Sports' FIFA video game series, including the weekly updates
6. Team line up with squad formation (X, Y coordinates)
7. Betting odds from up to 10 providers
8. Detailed match events (goal types, possession, corner, cross, fouls, cards etc…) for +10,000 matches

