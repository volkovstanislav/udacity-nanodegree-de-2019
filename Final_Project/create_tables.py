create_immigration_table = """    
    create table if not exists public.immigration (
        i94yr integer not null,
        i94mon integer not null,
        i94cit integer not null,
        i94port varchar(256) not null,
        i94mode integer not null,
        i94visa integer not null,
        count integer,
        primary key(i94yr,i94mon,i94cit,i94port,i94mode,i94visa) 
    );
"""

create_airport_codes_table = """
    create table if not exists public.airport_codes (
        iata_code varchar(256),
        type varchar(256),
        name varchar(256),
        elevation_ft integer,
        iso_country varchar(256),
        iso_region varchar(256),
        municipality varchar(256),
        longitude float,
        latitude float,
        primary key (iata_code)
    );
"""
    
create_temperature_table = """
    create table if not exists public.temperature (
        dt datetime,
        averageTemperature float,
        averageTemperatureUncertainty float,
        city varchar(256),
        country varchar(256),
        latitude varchar(256),
        longitude varchar(256),
        primary key (dt, city)
    );
"""

create_demog_main_table = """
    create table if not exists public.demog_main (
        city varchar(256),                     
        state varchar(256),                    
        median_age float,             
        male_population integer,           
        female_population integer,  
        total_population integer,          
        number_of_veterans integer,        
        foreign_born integer,             
        average_household_size float,    
        state_code varchar(256) not null,
        primary key (state_code)
    );
"""

create_demog_race_table = """
    create table if not exists public.demog_race (
        state varchar(256),
        state_code varchar(256),
        city varchar(256),
        race varchar(256),
        count integer,
        primary key (state_code, race)
    );
"""
    
create_model_val_table = """
    create table if not exists model_val (
        model_id integer,
        model_val varchar(256),
        primary key (model_id)
    );
"""      
    
create_visa_val_table = """
    create table if not exists visa_val (
        visa_id integer,
        visa_val varchar(256),
        primary key (visa_id)
    );
"""  

create_state_val_table = """
    create table if not exists state_val (
        state_code varchar(256),
        state_val varchar(256),
        primary key (state_code)
    );
"""

create_country_val_table = """
    create table if not exists country_val (
        country_id integer,
        country_val varchar(256),
        primary key (country_id)
    );
"""

drop_immigration_table = "drop table if exists immigration;"
drop_airport_codes_table = "drop table if exists airport_codes;"
drop_temperature_table = "drop table if exists temperature;"
drop_demog_main_table = "drop table if exists demog_main;"
drop_demog_race_table = "drop table if exists demog_race;"
drop_model_val_table = "drop table if exists model_val;"
drop_visa_val_table = "drop table if exists visa_val;"
drop_state_val_table = "drop table if exists state_val;"
drop_country_val_table = "drop table if exists country_val;"