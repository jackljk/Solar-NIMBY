import geopandas as gpd
import pandas as pd
from helpers import *

state_abbreviations = get_state_abbr()


#### WIND CLEANING #####
def get_wind(datapath, fixed_BB):
    """
    Function to get the wind data

    datapath: path to the wind original wind data (i.e the shapefile)
    fixed_BB: the fixed bounding box dataframe
    """
    data = gpd.read_file(datapath)
    wind_sum_mw = (
        data[["county", "statename", "wind_mw"]]
        .groupby(["statename", "county"])
        .sum()
        .reset_index()
        .rename(
            columns={
                "statename": "state",
                "county": "county",
                "wind_mw": "total_wind_mw",
            }
        )
    )
    wind_avg_mw = (
        data[["county", "statename", "wind_mw"]]
        .groupby(["statename", "county"])
        .mean()
        .reset_index()
        .rename(
            columns={"statename": "state", "county": "county", "wind_mw": "avg_wind_mw"}
        )
    )
    wind_project_count = (
        data.groupby(["statename", "county"])
        .count()
        .reset_index()[["statename", "county", "plant_code"]]
        .rename(
            columns={"statename": "state", "county": "county", "plant_code": "count"}
        )
    )

    wind_df = (
        fixed_BB.merge(wind_sum_mw, on=["state", "county"], how="left")
        .merge(wind_avg_mw, on=["state", "county"], how="left")
        .merge(wind_project_count, on=["state", "county"], how="left")
        .fillna(0)
    )

    wind_df["Wind Capacity Intensity (MW / 1000 sq mile)"] = (
        wind_df["total_wind_mw"] / wind_df["area mi2"] * 1000
    )
    wind_df["Wind Project Intensity (Projects / 1000 sq mile)"] = (
        wind_df["count"] / wind_df["area mi2"] * 1000
    )

    return wind_df


#### GDP CLEANING #####


def get_GDP(datapath, fixed_BB):
    """
    Function to get the GDP data normalized by area

    datapath: path to the GDP data (cleaned csv)
    fixed_BB: the fixed bounding box dataframe
    """
    gdp_data = pd.read_csv(datapath)
    gdp_data["Description"] = gdp_data["Description"].str.strip()
    norm_real_GDP = gdp_data[
        gdp_data["Description"] == "Real GDP (thousands of chained 2017 dollars)"
    ]
    norm_real_GDP = fixed_BB.merge(
        norm_real_GDP, on=["state", "county"], how="left"
    ).fillna(0)
    norm_real_GDP = norm_real_GDP[["area mi2"] + list(gdp_data.columns.values)]
    norm_real_GDP = norm_real_GDP.drop(columns=["Description"])
    
    # Rename the cols from year to GDP_<year>
    curr_cols = list(norm_real_GDP.columns)
    cols = ["GDP_2017", "GDP_2018", "GDP_2019", "GDP_2020", "GDP_2021", "GDP_2022"]
    
    for i in range(len(cols)):
        norm_real_GDP = norm_real_GDP.rename(columns={curr_cols[i+1]: cols[i]})
    
    # divide all numerical columns by county area
    for col in cols:
        norm_real_GDP[col] = norm_real_GDP[col].astype("float32")
        norm_real_GDP[col] = norm_real_GDP[col] / (norm_real_GDP["area mi2"])
        # round to 2 decimal places
        norm_real_GDP[col] = norm_real_GDP[col].round(2)

    return norm_real_GDP


#### Solar Cleaning #####


def get_solar(datapath, fixed_BB):
    """
    Function to get the solar data normalized by area

    datapath: path to the solar data (from your inital cleaning process i.e "US Counties Deply Metrics Technoecon")
        - send me the original solar and I can use that instead to make it cleaner
    fixed_BB: the fixed bounding box dataframe
    """
    solar = pd.read_csv(datapath)
    solar_cols = solar[
        ["state", "county", "solar_mw_sum", "solar_mw_avg", "solar_mw_count"]
    ]
    solar_with_area = solar_cols.merge(fixed_BB, on=["state", "county"]).drop(
        columns=["GEOID"]
    )
    solar_with_area["Solar MW 1000 sq mile"] = (
        solar_with_area["solar_mw_sum"] / solar_with_area["area mi2"]
    )
    solar_with_area["Solar Projects 1000 sq mile"] = (
        solar_with_area["solar_mw_count"] / solar_with_area["area mi2"]
    )

    return solar_with_area


#### Education Level Cleaning #####


def get_education_18_24(datapath):
    data = pd.read_csv(datapath)

    # make the first row the header as first row contains more meaningful column names
    data.columns = data.iloc[0]
    data = data[1:]
    # drop the last column due to random nan value
    data = data.iloc[:, :-1]

    important_columns_18_24 = (
        data.columns.str.contains("Estimate")
        & data.columns.str.contains("18 to 24 years")
        & data.columns.str.contains("Percent")
        & ~data.columns.str.contains("Male")
        & ~data.columns.str.contains("Female")
    )

    rename_dict = {
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years!!Less than high school graduate": "18-24 Less than high school graduate",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years!!High school graduate (includes equivalency)": "18-24 High school graduate",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years!!Some college or associate's degree": "18-24 Some college or associate's degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years!!Bachelor's degree or higher": "18-24 Bachelor's degree or higher",
    }

    # get all columns that include the word 'estimate' and '18 to 24 years'
    data_18_24_estimates = data.loc[:, important_columns_18_24]
    state_counties = data.loc[:, ["Geographic Area Name"]]
    state_counties["state"] = state_counties["Geographic Area Name"].apply(
        lambda x: x.split(", ")[1].split("!!")[0]
    )
    state_counties["county"] = state_counties["Geographic Area Name"].apply(
        lambda x: " ".join(x.split(", ")[0].split(" ")[:-1]).rstrip()
    )

    data_18_24_estimates = (
        pd.concat([state_counties, data_18_24_estimates], axis=1)
        .drop(
            columns=[
                "Geographic Area Name",
                "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 18 to 24 years",
            ],
            axis=1,
        )
        .rename(columns=rename_dict)
    )

    return data_18_24_estimates


def get_education_25_over(datapath):
    data = pd.read_csv(datapath)

    # make the first row the header as first row contains more meaningful column names
    data.columns = data.iloc[0]
    data = data[1:]
    # drop the last column due to random nan value
    data = data.iloc[:, :-1]

    important_columns_25_over = (
        data.columns.str.contains("Estimate")
        & data.columns.str.contains("25 years")
        & data.columns.str.contains("Percent")
        & ~data.columns.str.contains("Male")
        & ~data.columns.str.contains("Female")
        & ~data.columns.str.contains("MEDIAN")
    )

    rename_dict_25_over = {
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Less than 9th grade": "25+ Less than 9th grade",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!9th to 12th grade, no diploma": "25+ 9th to 12th grade, no diploma",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!High school graduate (includes equivalency)": "25+ High school graduate",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Some college, no degree": "25+ Some college, no degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Associate's degree": "25+ Associate's degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Bachelor's degree": "25+ Bachelor's degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Graduate or professional degree": "25+ Graduate or professional degree",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!High school graduate or higher": "25+ High school graduate or higher",
        "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over!!Bachelor's degree or higher": "25+ Bachelor's degree or higher",
    }

    # get all columns that include the word 'estimate' and '18 to 24 years'
    data_25_over_estimates = data.loc[:, important_columns_25_over]
    state_counties = data.loc[:, ["Geographic Area Name"]]
    state_counties["state"] = state_counties["Geographic Area Name"].apply(
        lambda x: x.split(", ")[1].split("!!")[0]
    )
    state_counties["county"] = state_counties["Geographic Area Name"].apply(
        lambda x: " ".join(x.split(", ")[0].split(" ")[:-1]).rstrip()
    )

    data_25_over_estimates = (
        pd.concat([state_counties, data_25_over_estimates], axis=1)
        .drop(
            columns=[
                "Geographic Area Name",
                "Estimate!!Percent!!AGE BY EDUCATIONAL ATTAINMENT!!Population 25 years and over",
            ],
            axis=1,
        )
        .rename(columns=rename_dict_25_over)
    )

    return data_25_over_estimates


#### Private Schools #####
def get_no_priv_schools(datapath):
    data = pd.read_csv(datapath)
    data_clean = data[["NAME", "STATE", "NMCNTY"]].copy()

    # Replace all state abbr to fullname
    data_clean["STATE"] = data_clean["STATE"].map(state_abbreviations)

    # Remove "County" from county names
    data_clean["NMCNTY"] = data_clean["NMCNTY"].str.replace(" County", "")
    no_priv_sch = (
        data_clean.groupby(["STATE", "NMCNTY"])
        .count()
        .reset_index()[["STATE", "NMCNTY", "NAME"]]
        .rename(
            columns={
                "STATE": "state",
                "NMCNTY": "county",
                "NAME": "No. of Private Schools",
            }
        )
    )
    return no_priv_sch


#### Race Distribution #####
def get_race_dec(datapath):
    df = pd.read_csv(datapath)  # read the data
    df.columns = df.iloc[0]
    df = df[1:]
    col_of_interest = (
        list(df.columns[1:4])
        + [col for col in df.columns[:-1] if "Population of one race:!" in col]
        + [
            " !!Total:!!Not Hispanic or Latino:!!Population of two or more races:!!Population of two races:"
        ]
    )
    df_totals = df[col_of_interest]
    df_totals.columns = (
        df_totals.columns.str.replace("!!Total:", "Total")
        .str.replace("!!Not Hispanic or Latino", "")
        .str.strip()
    )
    df_totals = df_totals.set_index("Geographic Area Name")
    df_totals = df_totals.apply(to_int)
    df_totals = (
        df_totals.div(df_totals["Total"], axis=0).drop(columns=["Total"]).reset_index()
    )
    df_totals["state"] = df_totals["Geographic Area Name"].apply(
        lambda x: x.split(", ")[1]
    )
    df_totals["county"] = (
        df_totals["Geographic Area Name"]
        .apply(lambda x: x.split(", ")[0])
        .str.replace(" County", "")
    )
    mapper = {
        "Total!!Hispanic or Latino": "Hispanic/Latino",
        "Total:!!Population of one race:!!White alone": "White",
        "Total:!!Population of one race:!!Black or African American alone": "Black/African American",
        "Total:!!Population of one race:!!American Indian and Alaska Native alone": "American Indian/Alaska Native",
        "Total:!!Population of one race:!!Asian alone": "Asian",
        "Total:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone": "Native Hawaiian/Other Pacific Islander",
        "Total:!!Population of one race:!!Some Other Race alone": "Others",
    }
    df_cleaned = df_totals.drop(columns=["Geographic Area Name"]).rename(columns=mapper)
    df_cleaned["Others"] = (
        df_cleaned["Others"]
        + df_cleaned[
            "Total:!!Population of two or more races:!!Population of two races:"
        ]
    )
    df_cleaned = df_cleaned.drop(
        columns=["Total:!!Population of two or more races:!!Population of two races:"]
    )
    df_cleaned["county"] = df_cleaned["county"].str.replace(" Parish", "")
    return df_cleaned


def get_race_acs(datapath):
    df = pd.read_csv(datapath)  # read the data
    # maje first row the header
    df.columns = df.iloc[0]
    df = df[1:].drop(columns=[df.columns[-1]])
    estimate_cols = [col for col in df.columns if "Estimate" in col]
    df_estimates = df[
        ["Geographic Area Name"] + estimate_cols[: len(estimate_cols) - 2]
    ]
    df_estimates.columns = df_estimates.columns.str.replace(
        "Estimate!!", ""
    ).str.replace("Total:!!", "")
    # divide all columns by the total population
    df_estimates = df_estimates.set_index("Geographic Area Name")
    df_estimates = df_estimates.apply(to_int)
    df_estimates = (
        df_estimates.div(df_estimates["Total:"], axis=0)
        .drop(columns=["Total:"])
        .reset_index()
    )
    df_estimates["state"] = df_estimates["Geographic Area Name"].apply(
        lambda x: x.split(", ")[1]
    )
    df_estimates["county"] = (
        df_estimates["Geographic Area Name"]
        .apply(lambda x: x.split(", ")[0])
        .str.replace(" County", "")
    )
    mapper = {
        "Total:": "Total",
        "White alone": "White",
        "Black or African American alone": "Black/African American",
        "American Indian and Alaska Native alone": "American Indian/Alaska Native",
        "Asian alone": "Asian",
        "Native Hawaiian and Other Pacific Islander alone": "Native Hawaiian/Other Pacific Islander",
    }
    df_estimates["Other"] = (
        df_estimates["Some other race alone"] + df_estimates["Two or more races:"]
    )
    df_cleaned = df_estimates.drop(
        columns=["Geographic Area Name", "Some other race alone", "Two or more races:"]
    ).rename(columns=mapper)
    df_cleaned["county"] = df_cleaned["county"].str.replace(" Parish", "")
    return df_cleaned


#### Election Distribution #####


def get_election(datapath, party="all"):
    data = pd.read_csv(datapath)
    county_vote = data[
        [
            "state",
            "county_name",
            "candidate",
            "mode",
            "party",
            "candidatevotes",
            "totalvotes",
        ]
    ]

    county_vote = (
        county_vote.groupby(["state", "county_name", "party"]).sum().reset_index()
    )
    county_vote["state"] = county_vote["state"].str.capitalize()
    county_vote["county_name"] = county_vote["county_name"].str.capitalize()
    county_vote["percentage_vote"] = (
        county_vote["candidatevotes"] / county_vote["totalvotes"]
    )
    county_vote = county_vote.rename(columns={"county_name": "county"})
    county_vote = county_vote.drop(columns=["candidatevotes", "totalvotes"])

    county_vote_democrat = county_vote[county_vote["party"] == "DEMOCRAT"][
        ["state", "county", "percentage_vote"]
    ].rename({"percentage_vote": "democrat_percentage_vote"}, axis=1)
    county_vote_republican = county_vote[county_vote["party"] == "REPUBLICAN"][
        ["state", "county", "percentage_vote"]
    ].rename({"percentage_vote": "republican_percentage_vote"}, axis=1)
    county_vote_green = county_vote[county_vote["party"] == "GREEN"][
        ["state", "county", "percentage_vote"]
    ].rename({"percentage_vote": "green_percentage_vote"}, axis=1)
    county_vote_libertarian = county_vote[county_vote["party"] == "LIBERTARIAN"][
        ["state", "county", "percentage_vote"]
    ].rename({"percentage_vote": "libertarian_percentage_vote"}, axis=1)
    county_vote_other = county_vote[
        (county_vote["party"] != "DEMOCRAT")
        & (county_vote["party"] != "REPUBLICAN")
        & (county_vote["party"] != "GREEN")
        & (county_vote["party"] != "LIBERTARIAN")
    ][["state", "county", "percentage_vote"]].rename(
        {"percentage_vote": "other_percentage_vote"}, axis=1
    )

    if party == "all":
        data_dict = {
            "democrat": county_vote_democrat,
            "republican": county_vote_republican,
            "green": county_vote_green,
            "libertarian": county_vote_libertarian,
            "other": county_vote_other,
        }
        return data_dict

    if party == "Democrat":
        return county_vote_democrat
    elif party == "Republican":
        return county_vote_republican
    elif party == "Green":
        return county_vote_green
    elif party == "Libertarian":
        return county_vote_libertarian
    elif party == "Other":
        return county_vote_other
    else:
        return "Invalid party. Please choose from 'Democrat', 'Republican', 'Green', 'Libertarian', 'Other'"


#### Income Distribution #####
def get_income(datapath):
    data = pd.read_csv(datapath)
    # make first row the header
    data.columns = data.iloc[0]
    data = data[1:].drop(columns=["Geography"]).drop(columns=[data.columns[-1]])
    estimate_cols = [
        col
        for col in data.columns
        if "Estimate" in col and "Households" in col and "Median" in col
    ]
    df_estimates = data[["Geographic Area Name"] + estimate_cols]
    df_income_clean = df_estimates.rename(
        columns={"Estimate!!Households!!Median income (dollars)": "Median Income"}
    )
    df_income_clean["state"] = df_income_clean["Geographic Area Name"].apply(
        lambda x: x.split(", ")[1]
    )
    df_income_clean["county"] = (
        df_income_clean["Geographic Area Name"]
        .apply(lambda x: x.split(", ")[0])
        .str.replace(" County", "")
    )
    df_income_clean = df_income_clean.drop(columns=["Geographic Area Name"])
    # get rid of parish in the county column
    df_income_clean["county"] = df_income_clean["county"].str.replace(" Parish", "")
    return df_income_clean


#### Unemployment data #####
def get_unemployment(datapath):
    data = pd.read_csv(datapath)
    # make the first row the header
    data.columns = data.iloc[0]
    data = data[1:]
    # remove the nan which is the last column
    data = data.iloc[:, :-1]

    # Get the columns we want "unemployment rate"
    cols = [x for x in data.columns if 'Population 16 years and over' in x and "AGE" not in x and "RACE" not in x and "Labor" not in x]
    data = data[['Geography', 'Geographic Area Name'] + cols]
    data_important = data[['Geography', 'Geographic Area Name'] + ["Estimate!!Total!!Population 16 years and over", "Estimate!!Unemployment rate!!Population 16 years and over"]]
    
    data_important = data_important.rename(columns={"Estimate!!Total!!Population 16 years and over": "Total Unemployment", "Estimate!!Unemployment rate!!Population 16 years and over": "Unemployment Rate"})
    # split the geography column into state and county
    data_important['state'] = data_important['Geographic Area Name'].apply(lambda x: x.split(", ")[1])
    data_important['county'] = data_important['Geographic Area Name'].apply(lambda x: x.split(", ")[0])
    data_important = data_important.drop(columns=['Geography', 'Geographic Area Name'])
    # reorder columns
    data_important = data_important[['state', 'county', 'Total Unemployment', 'Unemployment Rate']]
    data_important['county'] = data_important['county'].str.replace(" County", "")
    
    return data_important