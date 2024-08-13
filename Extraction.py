import pandas as pd 

def run_extraction():
    try:
        df =pd.read_csv(r'C:\Users\ubior\Desktop\zipco_food_orchestraion_with_apache_airflow\clean_data.csv')
        print('data extrated sucessfully')
    except Exception as e:
        print(f'error as {e}')