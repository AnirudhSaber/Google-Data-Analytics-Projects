import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    df['started_at'] = pd.to_datetime(df['started_at'])
    df['ended_at'] = pd.to_datetime(df['ended_at'])

    df = df.drop_duplicates().reset_index(drop=True)

    df['tripduration']=df['ended_at'] - df['started_at']
    df['duration_in_minute'] =  df['tripduration'].dt.total_seconds()/60
    df.drop(df[df['duration_in_minute'] < 0].index, inplace = True)
    df['trip_id'] = df.index + 1

    start_datetime_dim = df[['started_at']].reset_index(drop=True)
    start_datetime_dim['started_hour'] = start_datetime_dim['started_at'].dt.hour
    start_datetime_dim['started_day'] = start_datetime_dim['started_at'].dt.day
    start_datetime_dim['started_month'] = start_datetime_dim['started_at'].dt.month
    start_datetime_dim['started_year'] = start_datetime_dim['started_at'].dt.year
    start_datetime_dim['started_weekday'] = start_datetime_dim['started_at'].dt.weekday
    start_datetime_dim['start_datetime_id'] = start_datetime_dim.index + 1
    start_datetime_dim.rename(columns = {"started_at":"stated_datetime"},inplace=True)
    start_datetime_dim = start_datetime_dim[['start_datetime_id','stated_datetime','started_hour','started_day','started_month','started_year','started_weekday']]

    end_datetime_dim = df[['ended_at']].reset_index(drop=True)
    end_datetime_dim['ended_hour'] = end_datetime_dim['ended_at'].dt.hour
    end_datetime_dim['ended_day'] = end_datetime_dim['ended_at'].dt.day
    end_datetime_dim['ended_month'] = end_datetime_dim['ended_at'].dt.month
    end_datetime_dim['ended_year'] = end_datetime_dim['ended_at'].dt.year
    end_datetime_dim['ended_weekday'] = end_datetime_dim['ended_at'].dt.weekday
    end_datetime_dim['end_datetime_id'] = end_datetime_dim.index + 1
    end_datetime_dim.rename(columns = {"ended_at":"ended_datetime"},inplace=True)
    end_datetime_dim = end_datetime_dim[['end_datetime_id','ended_datetime','ended_hour','ended_day','ended_month','ended_year','ended_weekday']]
    
    start_location_dim = df[['start_lat','start_lng']].reset_index(drop=True)
    start_location_dim['start_location_id'] = start_location_dim.index + 1
    start_location_dim = start_location_dim[['start_location_id','start_lat','start_lng']]
    
    end_location_dim = df[['end_lat','end_lng']].reset_index(drop=True)
    end_location_dim['end_location_id'] = end_location_dim.index + 1
    end_location_dim = end_location_dim[['end_location_id','end_lat','end_lng']]

    member_dim = df[['member_casual']].reset_index(drop=True)
    member_dim = member_dim.drop_duplicates().reset_index(drop=True)
    member_dim['member_id'] = member_dim.index + 1
    member_dim = member_dim[['member_id','member_casual']]

    rideable_type_dim = df[['rideable_type']].reset_index(drop=True)
    rideable_type_dim = rideable_type_dim.drop_duplicates().reset_index(drop=True)
    rideable_type_dim['rideable_type_id'] = rideable_type_dim.index + 1
    rideable_type_dim = rideable_type_dim[['rideable_type_id','rideable_type']]


    ride_details = df.merge(rideable_type_dim, left_on='rideable_type', right_on='rideable_type') \
                .merge(member_dim, left_on='member_casual', right_on='member_casual') \
                .merge(end_location_dim, left_on='trip_id', right_on='end_location_id') \
                .merge(start_location_dim, left_on='trip_id', right_on='start_location_id') \
                .merge(end_datetime_dim, left_on='trip_id', right_on='end_datetime_id') \
                .merge(start_datetime_dim, left_on='trip_id', right_on='start_datetime_id') \
                [['trip_id','ride_id','member_id','rideable_type_id','start_location_id','start_station_id','start_station_name','start_datetime_id','end_location_id','end_datetime_id','duration_in_minute','end_station_id','end_station_name']]

    return {"ride_details":ride_details.to_dict(orient="dict"),
    "start_datetime_dim":start_datetime_dim.to_dict(orient="dict"),
    "end_datetime_dim":end_datetime_dim.to_dict(orient="dict"),
    "start_location_dim":start_location_dim.to_dict(orient="dict"),
    "end_location_dim":end_location_dim.to_dict(orient="dict"),
    "member_dim":member_dim.to_dict(orient="dict"),
    "rideable_type_dim":rideable_type_dim.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
