#!/usr/bin/env python3
"""
Emergency Calls Enrichment Utility Functions

This module contains utility functions for processing emergency calls and agent activity data:
- Data loading and validation
- Agent session processing  
- Emergency call enrichment
- Analysis and reporting functions

The functions focus on data processing without verbose statistics or samples.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, to_timestamp, when, lead, lag,
    broadcast, desc, asc, expr
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("EmergencyCallsEnrichment") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()


def detect_double_timestamps(df, timestamp_columns, df_name):
    """
    Detect double timestamps with fractional parts and convert to int if no meaningful fractional parts exist
    
    Args:
        df: DataFrame to analyze and potentially convert
        timestamp_columns: List of timestamp column names to check
        df_name: Name of the dataset for logging
        
    Returns:
        DataFrame with converted columns where appropriate
    """
    converted_df = df
    
    for col_name in timestamp_columns:
        if col_name in df.columns:
            # Get the datatype of the column
            col_dtype = dict(df.dtypes)[col_name]
            
            # Check if datatype is double/float (not integer)
            if col_dtype in ['double', 'float']:
                # Find values that have fractional parts (would lose precision when converted to int)
                fractional_timestamps = df.filter(
                    (col(col_name).isNotNull()) & 
                    (col(col_name) != col(col_name).cast("bigint").cast("double"))  # Compare double vs int->double cast
                )
                
                count = fractional_timestamps.count()
                if count == 0:
                    # Convert entire column to integer since no fractional parts exist
                    converted_df = converted_df.withColumn(col_name, col(col_name).cast("bigint"))
    
    return converted_df


def load_emergency_calls(spark, file_path):
    """
    Load emergency calls data from CSV
    
    Args:
        spark: SparkSession
        file_path: Path to emergency_calls.csv
        
    Returns:
        DataFrame with emergency calls data
    """
    # Define schema for better performance and type safety
    schema = StructType([
        StructField("call_id", StringType(), True),
        StructField("ring_timestamp_unix", DoubleType(), True),
        StructField("pick_up_timestamp_unix", DoubleType(), True),
        StructField("call_end_timestamp", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("call_taker_station_id", StringType(), True)
    ])
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    
    # Detect double/float timestamps, convert to int if no fractional parts
    df = detect_double_timestamps(df, ["ring_timestamp_unix", "pick_up_timestamp_unix"], "Emergency Calls")
    
    # Single transformation: normalize, convert to timestamps, shift timezone, and calculate duration
    df = df.withColumn(
               "ring_timestamp", 
               expr("from_unixtime(CASE WHEN ring_timestamp_unix > 1e10 THEN ring_timestamp_unix / 1000 ELSE ring_timestamp_unix END) - interval 5 hours")
           ).withColumn(
               "pickup_timestamp",
               expr("from_unixtime(CASE WHEN pick_up_timestamp_unix > 1e10 THEN pick_up_timestamp_unix / 1000 ELSE pick_up_timestamp_unix END) - interval 5 hours") 
           ).withColumn(
               "call_duration_seconds",
               expr("unix_timestamp(call_end_timestamp) - unix_timestamp(pickup_timestamp)")
           )
    
    # Remove rows with null pickup timestamp, call end timestamp, or station
    df = df.filter(
        col("pick_up_timestamp_unix").isNotNull() &
        col("call_end_timestamp").isNotNull() &
        col("call_taker_station_id").isNotNull()
    )
    
    return df


def process_agent_sessions(agent_activity_df):
    """
    Comprehensive agent session processing:
    1. Fix incomplete sessions by adding synthetic logouts
    2. Validate login/logout chronological order
    3. Create agent sessions with null logout times for incomplete sessions
    
    Args:
        agent_activity_df: DataFrame with agent activity data
        
    Returns:
        DataFrame with agent session periods (login_time, logout_time, agent_id, station_id)
    """
    # Step 1: Fix incomplete sessions - add synthetic logouts for login->login sequences
    window_spec = Window.partitionBy("call_taker_id", "call_taker_station_id").orderBy("timestamp_parsed")
    
    df_with_next = agent_activity_df.withColumn("next_action", lead("action_type").over(window_spec)) \
                                   .withColumn("next_timestamp", lead("timestamp_parsed").over(window_spec))
    
    # Find login->login sequences (missing logout between logins)
    login_to_login = df_with_next.filter(
        (col("action_type") == "login") & 
        (col("next_action") == "login")
    )
    
    # Find last logins (logins with no subsequent action)
    last_logins = df_with_next.filter(
        (col("action_type") == "login") & 
        col("next_action").isNull()
    )
    
    # Create synthetic logout records for login->login sequences
    synthetic_logouts_between = login_to_login.select(
        col("call_taker_id"),
        expr("cast(next_timestamp - interval 1 second as string)").alias("timestamp"),
        expr("'logout'").alias("action_type"),
        col("call_taker_station_id"),
        expr("next_timestamp - interval 1 second").alias("timestamp_parsed")
    )
    
    # Create synthetic logout records with null timestamps for last logins
    synthetic_logouts_null = last_logins.select(
        col("call_taker_id"),
        expr("null").cast("string").alias("timestamp"),
        expr("'logout'").alias("action_type"),
        col("call_taker_station_id"),
        expr("null").cast("timestamp").alias("timestamp_parsed")
    )
    
    # Combine all synthetic logouts
    all_synthetic_logouts = synthetic_logouts_between.union(synthetic_logouts_null)
    
    # Combine original data with synthetic logouts
    cleaned_df = agent_activity_df.union(all_synthetic_logouts) \
                                 .orderBy("call_taker_id", "call_taker_station_id", "timestamp_parsed")
    
    # Step 2: Validate chronological order and remove invalid pairs
    df_with_next_clean = cleaned_df.withColumn("next_action", lead("action_type").over(window_spec)) \
                                   .withColumn("next_timestamp", lead("timestamp_parsed").over(window_spec))
    
    # Find login-logout pairs
    login_logout_pairs = df_with_next_clean.filter(
        (col("action_type") == "login") & 
        (col("next_action") == "logout")
    )
    
    # Remove invalid pairs where login >= logout
    invalid_pairs = login_logout_pairs.filter(
        col("timestamp_parsed") >= col("next_timestamp")
    )
    
    if invalid_pairs.count() > 0:
        # Remove problematic records
        problematic_logins = invalid_pairs.select(
            "call_taker_id", "call_taker_station_id", "timestamp_parsed"
        ).withColumnRenamed("timestamp_parsed", "problem_timestamp")
        
        problematic_logouts = invalid_pairs.select(
            "call_taker_id", "call_taker_station_id", "next_timestamp"
        ).withColumnRenamed("next_timestamp", "problem_timestamp")
        
        all_problematic = problematic_logins.union(problematic_logouts)
        
        validated_df = cleaned_df.join(
            all_problematic,
            (cleaned_df.call_taker_id == all_problematic.call_taker_id) &
            (cleaned_df.call_taker_station_id == all_problematic.call_taker_station_id) &
            (cleaned_df.timestamp_parsed == all_problematic.problem_timestamp),
            "left_anti"
        )
    else:
        validated_df = cleaned_df
    
    # Step 3: Create agent sessions with null logout times for incomplete sessions
    df_with_next_final = validated_df.withColumn("next_action", lead("action_type").over(window_spec)) \
                                     .withColumn("next_timestamp", lead("timestamp_parsed").over(window_spec))
    
    # Create sessions for all logins, with null logout_time if no logout exists
    sessions_df = df_with_next_final.filter(col("action_type") == "login").select(
        col("call_taker_id").alias("agent_id"),
        col("call_taker_station_id").alias("station_id"),
        col("timestamp_parsed").alias("login_time"),
        when(col("next_action") == "logout", col("next_timestamp"))
        .otherwise(None)
        .alias("logout_time")
    )
    
    return sessions_df


def validate_emergency_calls_stations(emergency_calls_df, agent_activity_df):
    """
    Validate that emergency calls have stations that exist in agent activity data
    Keeps calls with null stations for analysis purposes
    
    Args:
        emergency_calls_df: DataFrame with emergency calls data
        agent_activity_df: DataFrame with agent activity data
        
    Returns:
        DataFrame with emergency calls (keeps null stations, removes only invalid non-null stations)
    """
    # Get unique stations from agent activity
    valid_stations = agent_activity_df.select("call_taker_station_id").distinct()
    
    # Keep calls with null stations AND calls with valid stations
    # Remove only calls with non-null stations that don't exist in agent activity
    validated_df = emergency_calls_df.filter(
        col("call_taker_station_id").isNull() |  # Keep null stations
        col("call_taker_station_id").isin(  # Keep valid stations
            [row.call_taker_station_id for row in valid_stations.collect()]
        )
    )
    
    return validated_df


def load_agent_activity(spark, file_path):
    """
    Load agent activity data from CSV
    
    Args:
        spark: SparkSession
        file_path: Path to agent_activity.csv
        
    Returns:
        DataFrame with agent activity data
    """
    # Define schema
    schema = StructType([
        StructField("call_taker_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("action_type", StringType(), True),
        StructField("call_taker_station_id", StringType(), True)
    ])
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    
    # Convert timestamp to proper timestamp type
    df = df.withColumn("timestamp_parsed", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    return df


def enrich_calls_with_agents(emergency_calls_df, agent_sessions_df):
    """
    Enrich emergency calls with agent information
    
    Args:
        emergency_calls_df: DataFrame with emergency calls
        agent_sessions_df: DataFrame with agent session periods
        
    Returns:
        DataFrame with emergency calls enriched with agent information
    """
    # Join conditions:
    # 1. Station must match
    # 2. Call pick-up time must be within agent's session period (using UTC-5 timestamps)
    join_condition = (
        (emergency_calls_df.call_taker_station_id == agent_sessions_df.station_id) &
        (emergency_calls_df.pickup_timestamp >= agent_sessions_df.login_time) &
        ((agent_sessions_df.logout_time.isNull()) | (emergency_calls_df.pickup_timestamp < agent_sessions_df.logout_time))
    )
    
    # Perform the join
    enriched_df = emergency_calls_df.join(
        broadcast(agent_sessions_df),  # Broadcast smaller dataset for performance
        join_condition,
        "left"  # Left join to keep all calls, even if no agent is found
    )
    
    # Calculate total number of agents on shift at call time
    # Create a broadcast join to count all agents active at each call's pickup time
    from pyspark.sql.functions import count, when
    
    # For each call, count how many agents are on shift at that time
    agents_on_shift_condition = (
        (emergency_calls_df.pickup_timestamp >= agent_sessions_df.login_time) &
        ((agent_sessions_df.logout_time.isNull()) | (emergency_calls_df.pickup_timestamp < agent_sessions_df.logout_time))
    )
    
    # Join to count all active agents per call
    agents_count_df = emergency_calls_df.join(
        broadcast(agent_sessions_df.select("agent_id", "login_time", "logout_time")),
        agents_on_shift_condition,
        "left"
    ).groupBy(
        emergency_calls_df.call_id,
        emergency_calls_df.ring_timestamp_unix,
        emergency_calls_df.pick_up_timestamp_unix,
        emergency_calls_df.call_end_timestamp,
        emergency_calls_df.latitude,
        emergency_calls_df.longitude,
        emergency_calls_df.call_taker_station_id,
        emergency_calls_df.ring_timestamp,
        emergency_calls_df.pickup_timestamp,
        emergency_calls_df.call_duration_seconds
    ).agg(
        count(agent_sessions_df.agent_id).alias("total_number_of_agents")
    )
    
    # Join back with the enriched data to get the assigned agent
    final_result = agents_count_df.join(
        enriched_df.select("call_id", "agent_id"),
        "call_id",
        "left"
    )
    
    # Select final columns
    result_df = final_result.select(
        "call_id",
        "ring_timestamp_unix",
        "pick_up_timestamp_unix", 
        "call_end_timestamp",
        "latitude",
        "longitude",
        "call_taker_station_id",
        "agent_id",
        "ring_timestamp",
        "pickup_timestamp",
        "call_duration_seconds",
        "total_number_of_agents"
    )
    
    return result_df


def analyze_unassigned_calls(enriched_calls_df, agent_sessions_df):
    """
    Analyze why calls remain unassigned to agents
    
    Args:
        enriched_calls_df: DataFrame with enriched emergency calls
        agent_sessions_df: DataFrame with agent session periods
        
    Returns:
        None (prints analysis report)
    """
    print(f"\n=== Analysis of Unassigned Calls ===")
    
    # Get unassigned calls
    unassigned_calls = enriched_calls_df.filter(col("agent_id").isNull())
    total_calls = enriched_calls_df.count()
    unassigned_count = unassigned_calls.count()
    assigned_count = total_calls - unassigned_count
    
    print(f"Total calls: {total_calls}")
    print(f"Assigned calls: {assigned_count}")
    print(f"Unassigned calls: {unassigned_count}")
    
    # Count calls with null stations (no station pairing)
    calls_with_null_stations = enriched_calls_df.filter(col("call_taker_station_id").isNull())
    null_station_count = calls_with_null_stations.count()
    print(f"Calls with null stations (no station pairing): {null_station_count}")
    
    # Get calls with non-null stations but no agent assigned
    calls_with_stations_no_agent = unassigned_calls.filter(col("call_taker_station_id").isNotNull())
    station_no_agent_count = calls_with_stations_no_agent.count()
    print(f"Calls with stations but no agent assigned: {station_no_agent_count}")
    
    # Combine all unassigned calls (null stations + valid stations with no agents)
    all_unassigned_calls = calls_with_null_stations.union(calls_with_stations_no_agent)
    
    # Print all unassigned calls together
    if unassigned_count > 0:
        print(f"\nAll unassigned calls (null stations + no agent assigned):")
        all_unassigned_calls.select(
            "call_id", 
            "ring_timestamp_unix", 
            "pick_up_timestamp_unix", 
            "call_end_timestamp",
            "call_taker_station_id",
            "agent_id"
        ).show(unassigned_count, truncate=False)
    
    # Get unassigned calls that have valid stations but no agent sessions during call time
    unassigned_calls = enriched_calls_df.filter(col("agent_id").isNull())
    stations_with_agents = agent_sessions_df.select("station_id").distinct()
    
    # Find unassigned calls at stations that DO have agent sessions (but calls are outside session periods)
    calls_outside_sessions = unassigned_calls.join(
        stations_with_agents,
        unassigned_calls.call_taker_station_id == stations_with_agents.station_id,
        "inner"
    )
    
    calls_outside_sessions_count = calls_outside_sessions.count()
    
    print(f"Total calls outside agent session periods: {calls_outside_sessions_count}")
    
    if calls_outside_sessions_count > 0:
        print(f"\nAll calls outside agent session periods:")
        calls_outside_sessions.select(
            "call_id", 
            "ring_timestamp",
            "pickup_timestamp",
            "call_end_timestamp",
            "call_taker_station_id",
            "agent_id"
        ).show(calls_outside_sessions_count, truncate=False)
    
    print(f"=== End Outside Session Periods Analysis ===\n")


