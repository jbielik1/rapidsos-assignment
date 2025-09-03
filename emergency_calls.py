#!/usr/bin/env python3
"""
Emergency Calls Enrichment Pipeline

This script orchestrates the complete data processing pipeline:
1. Loads emergency calls and agent activity data
2. Processes and validates agent sessions  
3. Enriches emergency calls with agent information
4. Analyzes unassigned calls
5. Saves processed data to CSV
"""

import os
import shutil
import glob
from pyspark.sql import SparkSession
from util_functions import (
    clean_emergency_calls,
    load_emergency_calls,
    load_agent_activity,
    process_agent_sessions,
    validate_emergency_calls_stations,
    enrich_calls_with_agents,
    analyze_unassigned_calls
)


def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("EmergencyCallsEnrichment") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()


def main():
    """Main execution function"""
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        print("Loading emergency calls data...")
        emergency_calls_df = load_emergency_calls(spark, "data/emergency_calls.csv")
        print(f"Loaded {emergency_calls_df.count()} emergency calls")
        
        print("Cleaning emergency calls data...")
        emergency_calls_df = clean_emergency_calls(emergency_calls_df)
        print(f"Cleaned emergency calls: {emergency_calls_df.count()}")
        
        print("Loading agent activity data...")
        agent_activity_df = load_agent_activity(spark, "data/agent_activity.csv")
        print(f"Loaded {agent_activity_df.count()} agent activity records")
        
        print("Processing agent sessions (fixing incomplete, validating order, creating sessions)...")
        agent_sessions_df = process_agent_sessions(agent_activity_df)
        print(f"Created {agent_sessions_df.count()} agent session periods")
        
        print("Validating emergency calls stations against agent activity...")
        emergency_calls_df = validate_emergency_calls_stations(emergency_calls_df, agent_activity_df)
        print(f"Validated emergency calls: {emergency_calls_df.count()}")
        
        print("Enriching emergency calls with agent information...")
        enriched_calls_df = enrich_calls_with_agents(emergency_calls_df, agent_sessions_df)
        
        # Analyze why calls remain unassigned
        analyze_unassigned_calls(enriched_calls_df, agent_sessions_df)
        
        # Save results to CSV
        print("Saving enriched data to output/processed_emergency_data.csv...")
        
        # Create output directory if it doesn't exist
        os.makedirs("output", exist_ok=True)
        
        # Save as single CSV file with proper naming
        temp_path = "output/temp_processed_emergency_data"
        enriched_calls_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
        
        # Move the part file to the final CSV name
        # Find the generated part file
        part_files = glob.glob(f"{temp_path}/part-*.csv")
        if part_files:
            shutil.move(part_files[0], "output/processed_emergency_data.csv")
            # Remove the temporary directory
            shutil.rmtree(temp_path)
        
        print("Data successfully saved!")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
