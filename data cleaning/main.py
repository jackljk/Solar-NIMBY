import pandas as pd
from utils import *

datapaths = {
    "Wind": "data/wind/ez_gis.plant_power_eia_v8_wind.shp",
    "GDP": "data/gdp_data.csv",
    "Solar": "data/solar-(jenny's_data).csv",
    "education": "data/education_data.csv",
    "private_schools": "data/private_school_data.csv",
    "DEC_race": "data/race_dec_data.csv",
    "ACS_race": "data/race_acs_data.csv",
    "election": "data/election_data.csv",
    "income": "data/income_data.csv",
    "unemployment": "data/unemployment_data.csv",
}

bounding_box = pd.read_csv("data/county_bounding_boxes.csv")

def load_data(
    race_type = 'DEC', election_type = 'Democrat', education_type = '18-24'
):
    
    # Normalized data
    wind = get_wind(datapaths['Wind'], bounding_box)
    gdp = get_GDP(datapaths['GDP'], bounding_box)
    solar = get_solar(datapaths['Solar'], bounding_box)
    
    # Non-normalized data
    private_schools = get_no_priv_schools(datapaths['private_schools'])
    income = get_income(datapaths['income'])
    unemployment = get_unemployment(datapaths['unemployment'])
    
    # Race
    if race_type == 'DEC':
        race = get_race_dec(datapaths["DEC_race"])
    elif race_type == 'ACS':
        race = get_race_acs(datapaths["ACS_race"])
    else:
        raise ValueError(f"Invalid data type: {race_type}")
        
    # Election
    if election_type == 'Democrat':
        election = get_election(datapaths["election"], party='Democrat')
    elif election_type == 'Republican':
        election = get_election(datapaths["election"], party='Republican')
    elif election_type == 'Other':
        election = get_election(datapaths["election"], party='Other')
    elif election_type == 'Green':
        election = get_election(datapaths["election"], party='Green')
    elif election_type == 'Libertarian':
        election = get_election(datapaths["election"], party='Libertarian')
    elif election_type == 'all':
        # Get all election data returned as a dictionary
        election = get_election(datapaths["election"], party='all')
    else:
        raise ValueError(f"Invalid party type: {election_type}")
        
    # Education
    if education_type == "18-24":
        education = get_education_18_24(datapaths["education"])
    elif education_type == "25+":
        education = get_education_25_over(datapaths["education"])
    elif education_type == "all":
        education_18_24 = get_education_18_24(datapaths["education"])
        education_25_over = get_education_25_over(datapaths["education"])
    else:
        raise ValueError(f"Invalid education type: {education_type}")
    
    # Merge all normalized data
    merged = merged_normalized_data(wind, gdp, solar)
    
    # Merge Private Schools
    merged = merged.merge(private_schools, on=['county', 'state'], how='outer')
    
    # Merge Income
    merged = merged.merge(income, on=['county', 'state'], how='outer')
    
    # Merged Unemployment
    merged = merged.merge(unemployment, on=['county', 'state'], how='outer')
    
    # Merge Race
    merged = merged.merge(race, on=['county', 'state'], how='outer')
    
    # Merge Election
    if type(election) == dict:
        for key in election.keys():
            merged = merged.merge(election[key], on=['county', 'state'], how='outer')
    else:
        merged = merged.merge(election, on=['county', 'state'], how='outer')
        
    # Merge Education
    if education_type == "all":
        merged = merged.merge(education_18_24, on=['county', 'state'], how='outer')
        merged = merged.merge(education_25_over, on=['county', 'state'], how='outer')
    else:
        merged = merged.merge(education, on=['county', 'state'], how='outer')
    
    return merged
    
    
    
    
