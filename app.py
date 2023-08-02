from flask import Flask, request, jsonify
import pandas as pd
import psycopg2

app = Flask(__name__)


def load_data():
    users_df = pd.read_csv('data/users.csv')
    experiments_df = pd.read_csv('data/user_experiments.csv')
    compounds_df = pd.read_csv('data/compounds.csv')
    return users_df, experiments_df, compounds_df

def derive_features(users_df, experiments_df):
    # Derive features
    users_df['total_experiments'] = experiments_df.groupby('user_id')['experiment_id'].count()
    users_df['average_experiments'] = experiments_df.groupby('user_id')['experiment_id'].mean()

    # Most commonly experimented compound
    experiments_df['experiment_compound_ids'] = experiments_df['experiment_compound_ids'].str.split(';')
    experiments_df = experiments_df.explode('experiment_compound_ids')
    compound_counts = experiments_df['experiment_compound_ids'].value_counts()
    most_common_compound = compound_counts.idxmax()
    users_df['most_common_compound'] = most_common_compound

    return users_df

def upload_to_database(users_df):
    # Connect to PostgreSQL database and upload data
    conn = psycopg2.connect(
        host='localhost',
        user='qiangma',
        password='qiangma',
        dbname='etl'
    )
    cursor = conn.cursor()

    # Create a table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_data (
            user_id INT PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            signup_date DATE,
            total_experiments INT,
            average_experiments FLOAT,
            most_common_compound VARCHAR
        )
    ''')

    # Upload data
    for _, row in users_df.iterrows():
        cursor.execute('''
            INSERT INTO user_data (user_id, name, email, signup_date, total_experiments, average_experiments, most_common_compound)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (row['user_id'], row['name'], row['email'], row['signup_date'], row['total_experiments'], row['average_experiments'], row['most_common_compound']))

    # Commit and close connections
    conn.commit()
    cursor.close()
    conn.close()

def etl():
    # Load CSV files
    # Process files to derive features
    # Upload processed data into a database
    # Load and process CSV files

    users_df, experiments_df, _ = load_data()
    users_df = derive_features(users_df, experiments_df)
    upload_to_database(users_df)


# Your API that can be called to trigger your ETL process
# API endpoint to trigger the ETL process
@app.route('/trigger_etl', methods=['POST'])
def trigger_etl():
    # Trigger your ETL process here
    etl()
    return {"message": "ETL process started"}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)