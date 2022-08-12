import numpy as np
import pandas as pd
from datetime import date, timedelta
import tgfr

output_path = "C:\\Users\\peaju\\THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD\\DP - PC - Team 1 - Documents\\PC - Team 1\\2022\\python\\dash_board\\output\\"
data_path = "C:\\Users\\peaju\\THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD\\DP - PC - Team 1 - Documents\\PC - Team 1\\2022\\python\\dash_board\\data\\"
today = date.today()


def load_denorm(days = 90):
    """Load denorm data from TGFR server from previous specified days"""
    # Adjust to/from date
    from_param = str(today - timedelta(days=90))
    to_param = str(today)

    denorm_df = tgfr.get_denorm_data(from_param, to_param)

    # rename columns
    columns_name = {
        'arrival_aerodrome_icao_code' : 'arrival_aerodrome',
        'departure_aerodrome_icao_code' : 'departure_aerodrome',
        'alternate_aerodromes_icao_code' : 'alternate_aerodrome',
        'aircraft_aircraft_config_aircraft_version' : 'aircraft_type',
        'aircraft_aircraft_config_cabin_version' : 'aircraft_cabin_version',
        'aircraft_aircraft_config_fleet_name' : 'aircraft_fleet_name'
    }
    denorm_df.rename(columns=columns_name, inplace=True)
    return denorm_df

def load_eofp(n):
    """Load eofp from most recent up to n loop. Each loop will download 500 data"""
    eofp_df = pd.DataFrame()
    for i in np.arange(n):
        limit = 500
        skip = 500 * i
        chunk = tgfr.get_oefp_user_input(skip=skip, limit=limit)
        eofp_df = pd.concat([eofp_df, chunk], ignore_index= True)
    return eofp_df

def load_merged_df():
    return pd.read_json(data_path + 'merged_data.json')

def export_csv(df, name):
    df.to_csv(output_path + name +'.csv')

def export_json(df, name):
    df.to_json(output_path + name + '.json')

