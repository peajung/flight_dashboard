import pandas as pd
import numpy as np
from datetime import datetime
import json
from urllib.request import urlopen

def check_cf(cfvalue, cfmin, cfmax):
    if cfvalue > cfmax:
        return cfmax
    elif cfvalue < cfmin:
        return cfmin
    else:
        return cfvalue

def change_aircraft_regis(aircraft_regis):
    return aircraft_regis[0:2] + aircraft_regis[3:6]

def compare_and_choose(values_list, mean):
    """Ger list of values and return the values that closest to mean"""
    
    diff_max = mean
    if np.nansum(values_list) == 0:
        return np.nan
    else:
        for val in values_list:
            diff = abs(val - mean)
            if diff_max > diff:
                diff_max = diff
                closet_value = val
        return closet_value
        
def columns_extract(df, col_name):
    return df[col_name].apply(pd.Series)

def count_fuel_data(fuel_report_df):
    """Return DataFram contain number of Fuel report data each month"""
    
    fuel_summary_df = fuel_report_df.sort_values(['flight_date', 'flight_number']).reset_index()[['flight_date','flight_number','aircraft_registration','dep']]
    month = []
    year = []
    for date in fuel_summary_df.flight_date:
        date_time = pd.to_datetime(date)
        month.append(date_time.month)
        year.append(date_time.year)

    fuel_summary_df['month'] = month
    fuel_summary_df['year'] = year
    return fuel_summary_df.groupby(['year','month'])['flight_number'].count()

def create_fuelreport_df(denorm_df):
    fuel_report_df = denorm_df[denorm_df.fuelreport.notna()]['fuelreport'].apply(pd.Series)
    fuel_report_df.drop_duplicates(subset=['dep', 'flight_number', 'flight_date', 'aircraft_registration'], inplace=True)
    return fuel_report_df

def create_op_data(path = './data/OpMovement.xlsx',sheet = 'OpMovement',cols = 'A:H'):
    # read excel file
    op_data_df = pd.read_excel(path,sheet_name = sheet, usecols = cols)
    op_data_df['Date'] = op_data_df.Date.apply(pd.to_datetime)

    # append column month and year
    years = []
    months = []
    for date in op_data_df['Date']:
        years.append(date.year)
        months.append(date.month)

    op_data_df['year'] = years
    op_data_df['month'] = months
    op_data_df['Registration'] = 'HS-' + op_data_df.Registration
    return op_data_df

def create_tgfr_dataframe(path = './data/Survey.csv'):
    tgfr_df = pd.read_csv(path,sep=';')
    date_time = []
    for date in tgfr_df['flightDate']:
        date_time.append(pd.to_datetime(date))
    tgfr_df['flightDate'] = date_time
    tgfr_df[['departure', 'arrival']] = tgfr_df['flightSector'].str.split('-', expand=True)
    tgfr_columns = ['flightDate', 'flightNumber', 'acRegistration', 'departure', 'arrival',
        'plannedZFW', 'updatedZFW', 'ZFW',
        'plannedFlightTime', 
        'plannedTaxiFuel', 'plannedRampFuel', 'plannedTripFuel',
        'rampFuel','landingFuel','parkingFuel', 
        'offBlock', 'airborne', 'onGround', 'onBlock', 
        'takeOffFuel', 'planningType',
        'pfId', 'pmId', 'apuForParking']
    tgfr_df = tgfr_df[tgfr_columns]

def duration_to_hr(duration):
    if pd.isna(duration):
        return np.nan
    else:
        try:
            hr ,min, sec = duration.split(':')
            return int(hr) + int(min)/60 + int(sec)/3600
        except:
            return np.nan

def expand_columns(df, col_name):
    return df[col_name].apply(pd.Series)

def extra_fuel(corr, plan_ramp, plan_flt_time, dzfw):
    return (corr * dzfw /1000 * plan_flt_time) + plan_ramp

def four_digits_to_hr(digits):
    if pd.isna(digits):
        return np.nan
    else:
        return int(digits[0:2]) + int(digits[2:4])/60

def fuel_initiative_data(merged_df):
    """get eOFP data and return a DataFrame for fuel initiative analysis"""
    # Select some columns from merged_df
    eofp_data = merged_df[['flight_date', 'flight_number', "departure_aerodrome", 'arrival_aerodrome', 'aircraft_registration', 'planned_zfw']]
    eofp_data = eofp_data.join(merged_df.planned_fuel.apply(pd.Series)[['block_fuel','trip_fuel','taxi_fuel']])
    eofp_data = eofp_data.join(merged_df.fuelreport.apply(pd.Series).sample(20)[['std_date','plan_flt_time']])
    user_input_columns = merged_df.userInput.apply(pd.Series)[[
            'ramp_fuel',
            'est_zfw',
            'actual_zfw',
            'offblock_time',
            'offblock_fuel',
            'zfwcg',
            'pax_a',
            'pax_b',
            'pax_c',
            'pax_d',
            'pax_e',
            'infant',
            'airborne_time',
            'landing_time',
            'flight_time',
            'onblock_time',
            'onblock_fuel',
            'actual_burn_fuel',
            'block_time',
            'pf',
            'pm',
            'water_uplift',
            'water_remain',
        ]]
    eofp_data = eofp_data.join(user_input_columns)
    return eofp_data
    
# Get denormalized-flights data
def get_denorm_data(from_param, to_param) :
    denorm_url = 'http://sfuelepcn1.thaiairways.co.th:3001/denormalized-flights?'
    skip = 'skip=0'
    limit = 'limit=0'
    parameter = denorm_url + 'from='+from_param +'Z&to=' + to_param + 'Z&' + skip + '&' + limit
    denorm = pd.read_json(parameter)
    denorm.drop_duplicates(subset=['flight_date', 'flight_number', 'departure_aerodrome_icao_code', 'aircraft_registration'],inplace = True)
    return denorm

def get_dp_order(flight_plan, dp_name):
    dp = dp_name
    for i in reversed(np.arange(len(flight_plan['planned_check_point']['fixs']))):
        if dp == flight_plan['planned_check_point']['fixs'][i]['fixname']:
            return i

# User input eOFP (Set Limit = 1000, default = 50)
def get_oefp_user_input(skip = '0', limit = '500') :
    skip = str(skip)
    limit = str(limit)

    url = "https://tgeofp.rtsp.us/api/v1/userinputs?skip="+skip+"&limit="+limit
    return pd.read_json(url)

# Get all OFP data (Set limit = 1000, default = 150)
def get_ofp() :
    url = "https://tgeofp.rtsp.us/api/v1/ofp?limit=1000"
    return pd.read_json(url)

# Get JSON of ofp data by specific flightplan id
def get_ofp_by_flightplan(flightplan) :
    try:
        url = "https://tgeofp.rtsp.us/api/v1/ofp/" + flightplan
        response = urlopen(url)
        data = json.loads(response.read())
        return data
    except Exception as e:
        return None

def get_special_plan(flight_plan_json):
    """
    Get json flight plan data and return information of Special planning
    
    Parameter: flight_plan_json: json object
    
    return
        type of planing : ['normal', 'cf3', 'rcfp']  
        enroute alternate aerodrome name ICAO : str
        dp_name : str
        fuel_to_dp_plan :int
        trip fuel from DP to destination: int
        cf : int
    """
    try:
        type = flight_plan_json['special_planning']['_type']
        if type == 'PBR':
            dp_name = flight_plan_json['special_planning']['decision_point']['name']
            fuel_to_dp_plan = flight_plan_json['special_planning']['to_dp']['fuel']['value']
            enroute_alternate_aerodrome = flight_plan_json['special_planning']['enroute_alternate']['icao_id']
            dp_to_destination_trip_fuel = flight_plan_json['special_planning']['from_dp_to_arrival']['trip_fuel']['fuel']['value']
            dp_to_destination_cf = flight_plan_json['special_planning']['from_dp_to_arrival']['contingency_fuel']['fuel']['value']
            
            return 'rcfp', enroute_alternate_aerodrome, dp_name, fuel_to_dp_plan, dp_to_destination_trip_fuel, dp_to_destination_cf
        elif type == 'FEA':
            enroute_alternate_aerodrome = flight_plan_json['special_planning']['enroute_alternate']['icao_id']            
            return 'cf3', enroute_alternate_aerodrome,np.nan, np.nan,np.nan, np.nan
    except KeyError:
        return 'normal', np.nan, np.nan, np.nan, np.nan, np.nan

def merge_flightPlan_eofp(eofp,denorm) :
    """ger eofp dataFrame then use each flight plan to get information from flighplan database"""
    ## Initialize dataframe with eOFP
    df = eofp

    ## Get OFP data to create joint column on denorm
    for index, row in df.iterrows() :
        flightplan = df.iloc[index]["flightPlan"]
        
        # Get ofp flight information
        ofp_json = get_ofp_by_flightplan(flightplan)

        # Drop Unmatch FlightPlan ID
        if ofp_json == None : 
            df.drop(index = index, axis = 0)
            continue

        dep = ofp_json["flight_key"]['departure_aerodrome']['value']
        arr = ofp_json["flight_key"]['arrival_aerodrome']['value']
        flt_no = "THA" + ofp_json["flight_key"]["flight_number"]
        flt_date = datetime.strptime(ofp_json["flight_key"]["flight_date"],"%Y-%m-%dZ")
        flt_date = flt_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        ac_reg = ofp_json["aircraft"]["aircraft_registration"]
        alt_plan = ofp_json["average_altitude"]['value']
        aircraft_registration = ac_reg[:2] + "-" + ac_reg[2:]

        imported_time = datetime.strptime(ofp_json["imported_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        #imported_time = imported_time.strftime("%Y-%m-%dT%H:%M:%S.000Z") ## TO String

        # Insert new column
        df.loc[index, "departure_aerodrome"] = dep
        df.loc[index, "arrival_aerodrome"] = arr
        df.loc[index, "flight_number"] = flt_no
        df.loc[index, "flight_date"] = flt_date
        df.loc[index, "aircraft_registration"] = aircraft_registration
        df.loc[index, "ofp_imported_time"] = imported_time
    
    ## Trim only eOFP data with new inserted column
    data_list = [
        "flightPlan","userInput","plannedCheckPoint","createdAt","updatedAt","ofp_imported_time",
        "departure_aerodrome", "arrival_aerodrome",
        "aircraft_registration", "flight_date", "flight_number",
        ]
    df = df[data_list]

    ## Drop NAN rows and reset index
    df = df.dropna(how = 'all')
    df = df.reset_index()

    # Unique keys
    joint_list = ["departure_aerodrome", "arrival_aerodrome", "aircraft_registration", "flight_date", "flight_number"]

    # Sort & Drop duplicate
    df = df.sort_values(by = ["ofp_imported_time"])
    df = df.drop_duplicates( subset = joint_list, keep = "last") # Keep lastest ofp imported_time

    ## Merge with denormalized
    df = pd.merge(denorm, df, how = "left", on = joint_list)
    df = df.sort_values(by = ["flight_date"], ascending=False)
    return df

def nonnumeric_to_nan(x):
    try:
        float(x)
        return x
    except:
        return np.nan

def rcfp_impact(cf3, cf_plan, comp_fuel, flt_time, corr_factor, extra_fuel = 0,):
    return (cf3 - cf_plan - comp_fuel -extra_fuel)/1000 * flt_time * corr_factor



### function for data extraction ###



def load_denorm(from_param = 'from=2022-01-01T00:00:00.000Z',to_param = 'to=2022-06-30T23:59:59.000Z'):
    denorm_df = get_denorm_data(from_param, to_param)

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

def load_eofp(total_data):
    total_loop = total_data // 500 + 1

    # load first chunk of eOFP data
    eofp_df = get_oefp_user_input()

    for i in np.arange(total_loop):
        limit = 500
        skip = 500 + 500 * i
        chunk = get_oefp_user_input(skip=skip, limit=limit)
        eofp_df = pd.concat([eofp_df, chunk], ignore_index= True)
    return eofp_df

def load_special_plan(eofp_df_merged):
    special_columns = expend_columns(eofp_df_merged, 'special_planning')

    # initiate empty dict
    special_plan_dict = {
        'decision_point_name':[],
        'enroute_alternate_aerodrome':[],
        'fuel_to_dp':[],
        'min_fuel_dp':[],
        'plan_type':[]
    }
    for i, row in special_columns.iterrows():
        try:
            special_plan_dict['decision_point_name'].append(row['decision_point']['name'])
        except TypeError:
            special_plan_dict['decision_point_name'].append(np.nan)
        try:
            special_plan_dict['enroute_alternate_aerodrome'].append(row['enroute_alternate']['icao_id'])
        except TypeError:
            special_plan_dict['enroute_alternate_aerodrome'].append(np.nan)
        try:
            special_plan_dict['fuel_to_dp'].append(row['to_dp']['fuel']['value'])
        except TypeError:
            special_plan_dict['fuel_to_dp'].append(np.nan)
        try:
            special_plan_dict['min_fuel_dp'].append(row['min_fuel_to_proceed'])
        except TypeError:
            special_plan_dict['min_fuel_dp'].append(np.nan)
        try:
            special_plan_dict['plan_type'].append(row['_type'])
        except TypeError:
            special_plan_dict['plan_type'].append(np.nan)

    special_plan_df = pd.DataFrame(special_plan_dict, index=eofp_df_merged.index)
    special_plan_df['plan_type'].replace({np.nan:'Normal','FEW':'CF3', 'PBR':'RCFP'},inplace= True)
    return special_plan_df

def report_df_init(merged_df):
    return merged_df[[
        'flight_date',
        'flight_number',
        'aircraft_registration',
        'aircraft_type',
        'arrival_aerodrome',
        'departure_aerodrome',
        'alternate_aerodrome',
        'flight_plan_id',
        'planned_zfw',
        'dow'
    ]]

def plan_zfw(eofp_df_merged):
    zfw_df = pd.DataFrame(
        {
            'planned_zfw' : eofp_df_merged.planned_zfw,
            'zfw_plan_fuel_report' : eofp_df_merged.fuelreport.apply(pd.Series)['plan_zfw'].astype(float),
            'zfw_est_eofp' : eofp_df_merged.userInput.apply(pd.Series)['est_zfw'].astype(float)
        }
    )

    zfw_plan_list = []
    for i, row in zfw_df[['zfw_plan_fuel_report', 'planned_zfw']].iterrows():
        fuel_report_zfw = row['zfw_plan_fuel_report']
        planned_zfw = row['planned_zfw']
        if pd.isna(fuel_report_zfw) and pd.isna(planned_zfw):
            zfw_plan_list.append(np.nan)
        elif pd.isna(fuel_report_zfw):
            zfw_plan_list.append(planned_zfw)
        else:
            zfw_plan_list.append(fuel_report_zfw)

    zfw_df['zfw_plan'] = zfw_plan_list
    return zfw_df.zfw_plan, zfw_df.zfw_est_eofp
