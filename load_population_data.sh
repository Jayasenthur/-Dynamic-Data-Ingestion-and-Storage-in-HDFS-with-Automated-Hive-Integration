#!/bin/bash

# -------------------------------------------------------
# Population Data Loader with Database Verification
# -------------------------------------------------------

# Configuration
FILE_NAME="sub-est2023_44.csv"
CSV_URL="https://www2.census.gov/programs-surveys/popest/datasets/2020-2023/cities/totals/$FILE_NAME"
LOCAL_PATH="/home/hdoop/$FILE_NAME"
HDFS_PATH="/user/project/dataset/$FILE_NAME"
HIVE_DB="project_data"
HIVE_TABLE="population_data_2"
MAX_RETRIES=3
TIMEOUT_SEC=30

# Logging
LOG_FILE="${0%.*}.log"
exec > >(tee -a "$LOG_FILE") 2>&1

# Resource Monitoring
monitor_resources() {
    echo -e "\n--- SYSTEM RESOURCES $(date) ---"
    free -h | awk 'NR==1 || NR==2'
    df -h / | awk 'NR==1 || NR==2'
    echo "--------------------------------"
}

# Cleanup
cleanup() {
    echo -e "\n[Cleanup] Removing temporary files..."
    hadoop fs -rm -f "$HDFS_PATH" >/dev/null 2>&1 || true
    [ -f "$LOCAL_PATH" ] && rm -f "$LOCAL_PATH"
    echo "[Cleanup] Complete"
    exit 0
}
trap cleanup EXIT

# Verify/Create Hive Database
verify_hive_db() {
    echo -e "\n[Hive] Verifying database..."
    if ! hive -e "USE $HIVE_DB" >/dev/null 2>&1; then
        echo "[Hive] Creating database $HIVE_DB"
        hive -e "CREATE DATABASE IF NOT EXISTS $HIVE_DB" || {
            echo "[Hive] Failed to create database"
            return 1
        }
    fi
    echo "[Hive] Database verified"
    return 0
}

# Download File
download_file() {
    echo -e "\n[Download] Starting download..."
    for ((i=1; i<=MAX_RETRIES; i++)); do
        monitor_resources
        if wget \
            --timeout="$TIMEOUT_SEC" \
            --tries=1 \
            --header="User-Agent: Mozilla/5.0" \
            --header="Accept: text/html" \
            --referer="https://www.census.gov/" \
            "$CSV_URL" -O "$LOCAL_PATH"; then
            echo "[Download] Success: $LOCAL_PATH"
            return 0
        else
            echo "[Download] Attempt $i failed, retrying..."
            sleep 5
        fi
    done
    echo "[Download] Failed after $MAX_RETRIES attempts"
    return 1
}

# HDFS Operations
hdfs_operations() {
    echo -e "\n[HDFS] Starting operations..."
    monitor_resources
    
    if ! hadoop fs -ls / >/dev/null 2>&1; then
        echo "[HDFS] HDFS not available"
        return 1
    fi

    hadoop fs -mkdir -p "/user/project/dataset" || {
        echo "[HDFS] Failed to create directory"
        return 1
    }

    echo "[HDFS] Uploading to HDFS..."
    hadoop fs -put -f "$LOCAL_PATH" "$HDFS_PATH" || {
        echo "[HDFS] Upload failed"
        return 1
    }

    echo "[HDFS] Upload successful"
    return 0
}

# Hive Operations
hive_operations() {
    echo -e "\n[Hive] Starting operations..."
    monitor_resources

    echo "[Hive] Creating table..."
    hive -e "USE $HIVE_DB; CREATE TABLE IF NOT EXISTS $HIVE_TABLE (
        SUMLEV STRING, STATE STRING, COUNTY STRING, 
        PLACE STRING, COUSUB STRING, CONCIT STRING,
        PRIMGEO_FLAG STRING, FUNCSTAT STRING, 
        NAME STRING, STNAME STRING
    ) ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE" || {
        echo "[Hive] Table creation failed"
        return 1
    }

    echo "[Hive] Loading data..."
    hive -e "USE $HIVE_DB; LOAD DATA INPATH '$HDFS_PATH' INTO TABLE $HIVE_TABLE" || {
        echo "[Hive] Data load failed"
        return 1
    }

    echo "[Hive] Sample data:"
    hive -e "USE $HIVE_DB; SELECT * FROM $HIVE_TABLE LIMIT 5;"
}

# Main Execution
main() {
    echo -e "\n[Start] Pipeline started at $(date)"
    monitor_resources

    verify_hive_db || exit 1
    download_file || exit 1
    hdfs_operations || exit 1
    hive_operations || exit 1

    echo -e "\n[Success] Pipeline completed at $(date)"
}

main  explain to include in github documentation
