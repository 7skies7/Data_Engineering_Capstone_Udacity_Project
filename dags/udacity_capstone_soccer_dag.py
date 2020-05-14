from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 5, 3), 
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('udacity_capstone_soccer_dag',
          default_args=default_args,
          description='Load and transform soccer matches data in Redshift with Airflow',
          schedule_interval=None,
          max_active_runs=1,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_soccer_tables",
    dag=dag,
    sql='create_soccer_tables.sql',
    postgres_conn_id="redshift"
)


#Function to fetch the outcome of a match
def get_match_outcome(home_team, away_team, home_goal, away_goal, target):
    if home_goal == away_goal:
        return 'D'
    elif(home_goal > away_goal and target == home_team) or (home_goal<away_goal and target==away_team):
        return 'W'
    elif(home_goal > away_goal and target == away_team) or (home_goal<away_goal and target==home_team):
        return 'L'

def transform_match():    
        
    connstr = 'redshift+psycopg2://username:password@hostname:port/database'

    engine = create_engine(connstr) 
    print("Connection Established")
    
    with engine.connect() as conn, conn.begin():
        df = pd.read_sql("""
                        SELECT id, league_id, (SELECT name FROM League WHERE id = league_id LIMIT 1) league_name,
                        stage, season,date, match_api_id,
                        home_team_api_id, (SELECT team_long_name FROM Team WHERE team_api_id = home_team_api_id LIMIT 1) home_team, 
                        away_team_api_id, (SELECT team_long_name FROM Team WHERE team_api_id = away_team_api_id) away_team,
                        home_team_goal, away_team_goal, goal
                        FROM Match m
                        WHERE season like '2015%%'
                        ORDER BY date;
                        """, conn)

        #Transform the date column to support only date instead of datetime
        df['date'] = df['date'].apply(lambda x: x.split()[0])
        
        #Subset the columns concerning the stats of the home and away team & rename the column names
        away_teams = df[['id','league_id','league_name','stage','season','date','match_api_id','home_team_api_id','home_team','away_team_api_id','away_team',
                  'home_team_goal','away_team_goal']].copy(deep=True)
        away_teams['team_name_stats'] = away_teams['away_team']
        home_teams = df[['id','league_id','league_name','stage','season','date','match_api_id','home_team_api_id','home_team','away_team_api_id','away_team',
                  'home_team_goal','away_team_goal']].copy(deep=True)
        
        home_teams['team_name_stats'] = home_teams['home_team']
        home_teams.columns = ['id', 'league_id','league_name', 'stage', 'season', 'date', 'match_api_id','home_team_api_id', 'home_team', 'away_team_api_id',                                                                         'away_team','home_team_goal','away_team_goal','team_name_stats']
        away_teams.columns = ['id', 'league_id', 'league_name','stage', 'season', 'date', 'match_api_id', 'home_team_api_id', 'home_team', 'away_team_api_id', 'away_team',
       'home_team_goal', 'away_team_goal', 'team_name_stats']

        #Merge both the home & away datasets, order them by the date & the home team in order to get team stats consecutively
        league_team_stats = pd.concat([home_teams,away_teams]).sort_values(by=['date','home_team']).copy(deep=True)
        league_team_stats['outcome'] = league_team_stats.apply(lambda x: get_match_outcome(x['home_team'],x['away_team'],x['home_team_goal'],x['away_team_goal'],x['team_name_stats']),axis = 1)
        league_team_stats['points_earned'] = league_team_stats['outcome'].apply(lambda x: 3 if x=='W' else 0 if x=='L' else 1)
        league_team_stats['date'] = league_team_stats.date.map(lambda x: datetime.strptime(x,"%Y-%m-%d"))
        league_team_stats = league_team_stats.reset_index()
        
        #Calculate cumulative points, Goals for, Goal against & Goal difference for each team and at each stage of the season 
        league_team_stats['cum_points']=league_team_stats.groupby(['league_id','season','team_name_stats']).points_earned.cumsum()
        league_team_stats['goal_scored'] = league_team_stats.apply(lambda x: x['home_team_goal'] if x['team_name_stats']==x['home_team'] else x['away_team_goal'], axis=1)
        league_team_stats['goal_conceded'] = league_team_stats.apply(lambda x: x['away_team_goal'] if x['team_name_stats']==x['home_team'] else x['home_team_goal'], axis=1)
        league_team_stats['won'] = league_team_stats.apply(lambda x:1 if x['outcome']=='W' else 0,axis=1)
        league_team_stats['draw'] = league_team_stats.apply(lambda x:1 if x['outcome']=='D' else 0,axis=1)
        league_team_stats['loss'] = league_team_stats.apply(lambda x:1 if x['outcome']=='L' else 0,axis=1)
        league_team_stats['W'] = league_team_stats.groupby(['league_id','season','team_name_stats']).won.cumsum()
        league_team_stats['D'] = league_team_stats.groupby(['league_id','season','team_name_stats']).draw.cumsum()
        league_team_stats['L'] = league_team_stats.groupby(['league_id','season','team_name_stats']).loss.cumsum()
        league_team_stats['GF'] = league_team_stats.groupby(['league_id','season','team_name_stats']).goal_scored.cumsum()
        league_team_stats['GA'] = league_team_stats.groupby(['league_id','season','team_name_stats']).goal_conceded.cumsum()
        league_team_stats['GD'] = league_team_stats.apply( lambda x: x['GF']-x['GA'], axis=1)
        league_team_stats = league_team_stats.drop(["won","draw","loss"],axis=1)
        
        
        #Calculating the table position of each team at each stage of the tournament.
        league_team_stats['rank_calc'] = league_team_stats.apply(lambda x: x['cum_points']*10000 +x['GD']*50 +x['GF'],axis=1)
        league_team_stats['table_position'] = league_team_stats.groupby(['league_id', 'season', 'stage'])['rank_calc'].rank(ascending=False, method='min').astype(int)
        league_team_stats = league_team_stats.drop(['rank_calc','id','date', 'match_api_id', 'home_team_api_id', 'home_team', 'away_team_api_id',                                                                         'away_team','home_team_goal','away_team_goal','goal_scored','goal_conceded'],axis=1)
        print("Match Table Data Transformed to Include Points Stats")
        
        print("Load transformed data to points table in redshift")
        #print(league_team_stats.head(20).sort_values(['league_id','season','stage','table_position','team_name_stats'])[['league_name','season','stage','table_position']])
        league_team_stats.sort_values(['league_id','season','stage','table_position','team_name_stats'])[['league_name','season','stage','table_position','team_name_stats','W','D','L','GF','GA','GD','cum_points']].to_sql('points', conn, index=False, if_exists='replace')

        print("Points Table Added to Redshift")
        #return "Hello World"


load_country_to_redshift = StageToRedshiftOperator(
    task_id='Load_Country',
    table="country",
    s3_bucket = "udacity-dend-capstone-soccer",
    s3_key = "Country.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True,
)

load_league_to_redshift = StageToRedshiftOperator(
    task_id='Load_League',
    table="league",
    s3_bucket = "udacity-dend-capstone-soccer",
    s3_key = "League.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True,
)




load_player_to_redshift = StageToRedshiftOperator(
    task_id='Load_Player',
    table="player",
    s3_bucket = "udacity-dend-capstone-soccer",
    s3_key = "Player.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True,
)


load_player_attributes_to_redshift = StageToRedshiftOperator(
    task_id='Load_Player_attributes',
    table="player_attributes",
    s3_bucket = "udacity-dend-capstone-soccer",
    s3_key = "Player_Attributes.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True,
)

load_team_to_redshift = StageToRedshiftOperator(
    task_id='Load_Team',
    table="team",
    s3_bucket = "udacity-dend-capstone-soccer",
    s3_key = "Team.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True,
)


load_team_attributes_to_redshift = StageToRedshiftOperator(
    task_id='Load_team_attributes',
    table="team_attributes",
    s3_bucket = "udacity-dend-capstone-soccer",
    s3_key = "Team_attributes.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True,
)


load_match_to_redshift = StageToRedshiftOperator(
    task_id='Load_match',
    table="match",
    s3_bucket = "udacity-dend-capstone-soccer",
    s3_key = "Match.csv",
    file_format="CSV",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True,
)



transfrom_and_load_points_table = PythonOperator(
    task_id="transfrom_match_to_points",
    dag=dag,
    python_callable=transform_match
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["country", "league", "match", "team", "team_attributes", "player", "player_attributes", "points"]
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#List Dependencies For The Sparkify Data Pipline
start_operator >> create_tables_task >> [load_country_to_redshift, load_league_to_redshift, load_player_to_redshift, load_player_attributes_to_redshift, load_team_to_redshift, load_team_attributes_to_redshift, load_match_to_redshift] >> transfrom_and_load_points_table >> run_quality_checks >> end_operator
