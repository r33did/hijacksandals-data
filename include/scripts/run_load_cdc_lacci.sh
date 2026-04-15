#!/bin/bash
###############################################################################
# Script name  : daily_cdc_customer360.sh
# Description  : Run CDC LDM NDB LACCI Hive + Spark + Kafka pipeline
# Version      : v2.0 (enhanced for robustness & maintainability)
###############################################################################

set -euo pipefail

############################################
# CONFIGURATION
############################################
BEELINE="beeline"
HIVE_JDBC_URL="jdbc:hive2://udc2-datalake-lb.office.corp.indosat.com:10001/default;principal=hive/udc2-datalake-lb.office.corp.indosat.com@OFFICE.CORP.INDOSAT.COM"
HIVE_QUEUE="root.users.hdp-batch_user2"

SCRIPTDIR=$(dirname "$(readlink -f "$BASH_SOURCE")")
HOMEDIR=$(dirname "$SCRIPTDIR")

QUERYDIR="$HOMEDIR/queries"
LOGDIR="$HOMEDIR/logs"
KEYTABDIR="$HOMEDIR/keytab"
PSOWNER="hdp-batch_user2"
RECIPIENTS="ahmad.amrullah@ioh.co.id"

#YSDT=$(date -d "1 day ago" '+%Y%m%d')
#PSDT=$(date '+%Y%m%d')



#mkdir -p "$LOGDIR"

#KAFKA_BROKERS="xptkfkbrkr01.ioh.co.id:9093,xptkfkbrkr02.ioh.co.id:9093,xptkfkbrkr03.ioh.co.id:9093,xptkfkbrkr04.ioh.co.id:9093"
KAFKA_BROKERS="xdtkfkbrkr01.gammasprint.com:9093,xdtkfkbrkr02.gammasprint.com:9093,xdtkfkbrkr03.gammasprint.com:9093"
KAFKA_TOPIC="ldm-cdc-lacci-com"
KAFKA_CONFIG="${SCRIPTDIR}/client.properties"
HDFS_DIR="/user/hdp-batch_user2/cdc_ldm_lacci"


############################################
# FUNCTIONS
############################################
kinit_renew() {
    kinit -kt "${KEYTABDIR}/${PSOWNER}.keytab" "${PSOWNER}"
}

send_alert() {
    local message=$1
    curl -s -X POST http://hdp2-dwr0080:5501/send-html \
      -H "Content-Type: application/json" \
      -d "{
        \"to\": \"${RECIPIENTS}\",
        \"subject\": \"[ALERT] Daily CDC LDM Lacci NDB\",
        \"body\": \"The CDC job scheduled for today (${PSDT}) is <strong class=\\\"failed\\\">FAILED</strong>. ${message}\"
      }" || true
}

check_partition() {
    local table=$1
    echo "Checking partition for table: $table (dt_id=${PSDT})"

    kinit_renew
    local result
    result=$($BEELINE -u "$HIVE_JDBC_URL" --silent=true --outputformat=tsv2 \
        -e "SHOW PARTITIONS $table;" | grep -c "dt_id=${PSDT}" || true)

    if [[ $result -gt 0 ]]; then
        echo "OK: Table $table has partition dt_id=${PSDT}"
    else
        echo "ERROR: Table $table does NOT have partition dt_id=${PSDT}"
#        send_alert "Partition missing for ${table}"
        exit 1
    fi
}


validate_prior_dt() {
    local max_prior_dt
    max_prior_dt=$($BEELINE -u "$HIVE_JDBC_URL" -hiveconf tez.queue.name="$HIVE_QUEUE" --silent=true --outputformat=csv2 -e "SELECT MAX(dt_id) FROM stg.data_lacci_com WHERE dt_id < '${PSDT}';" 2>/dev/null | tail -1 | tr -d '"')
    if [[ -z "$max_prior_dt" || "$max_prior_dt" == "NULL" ]]; then
        echo "ERROR: Could not determine valid prior dt_id from stg.data_lacci_com"
        exit 1
    fi

    YSDT=$(date -d "${PSDT} -1 day" '+%Y%m%d')

    if [[ "$max_prior_dt" != "$YSDT" ]]; then
        echo "WARNING: YSDT ($YSDT) does not match max prior dt_id ($max_prior_dt). Overriding YSDT to $max_prior_dt"
        YSDT="$max_prior_dt"
    else
        echo "INFO: YSDT ($YSDT) validated successfully."
    fi

    export YSDT
}


run_query() {
    local query_file=$1
    local logfile="$LOGDIR/$(basename "$query_file" .hql)_$(date +%Y%m%d_%H%M%S).log"

    echo "--------------------------------------------------"
    echo "Running query: $query_file"
    echo "Log file: $logfile"
    echo "--------------------------------------------------"

    kinit_renew
    if $BEELINE -u "$HIVE_JDBC_URL" \
        -hiveconf tez.queue.name="$HIVE_QUEUE" \
        --hivevar dt_id="$PSDT" \
        --hivevar priordt_id="$YSDT" \
        -f "${QUERYDIR}/$query_file" > "$logfile" 2>&1; then
        echo "SUCCESS: $query_file completed. See $logfile"
    else
        echo "ERROR: $query_file failed. Check $logfile"
#        send_alert "Query failed: ${query_file}"
        exit 1
    fi
}

compare_actual_date() {
    local query_date="$1"
    local actual_date="$2"
    if [[ -z "$query_date" || "$query_date" == "NULL" ]]; then
    echo "CDC table empty or unreadable, proceeding with full run"
    query_date=""
    fi
    if [[ "$query_date" == "$actual_date" ]]; then
    echo "CDC table already up-to-date (dt_id=$query_date). Exiting."
    exit 0
    fi
}



check_cdc_done() {
    echo "Checking CDC completion marker for dt_id=${PSDT}"

    kinit_renew
    local cnt
    cnt=$($BEELINE -u "$HIVE_JDBC_URL" --silent=true --outputformat=tsv2 \
        -e "SHOW PARTITIONS stg.ldm_cdc_lacci_com_done;" \
        | grep -c "dt_id=${PSDT}" || true)

    if [[ "$cnt" -gt 0 ]]; then
        echo "CDC already completed for dt_id=${PSDT}. Exiting."
        exit 0
    fi
}


############################################
# MAIN
############################################
kinit_renew
PSDT=$(date '+%Y%m%d')
YSDT=$(date -d "${PSDT} -1 day" '+%Y%m%d')
#check_cdc_done
validate_prior_dt
check_partition "stg.data_lacci_com"

# Execute queries
run_query "cdc.hql"

# Spark job: generate CDC JSON
kinit_renew
if ! spark-submit \
    --keytab "${KEYTABDIR}/${PSOWNER}.keytab" \
    --principal "${PSOWNER}" \
    "${SCRIPTDIR}/batch-pyspark-cdc-lacci-to-json.py" \
    --prc-dt=$PSDT; then
#    send_alert "Spark job failed"
    exit 1
fi


# Check if HDFS dir has any files
if hdfs dfs -test -d "$HDFS_DIR" && hdfs dfs -count "$HDFS_DIR" | awk '{if($2+$3>0) exit 0; else exit 1}'; then
    if ! hdfs dfs -cat "$HDFS_DIR"/*.json | kafka-console-producer \
        --bootstrap-server "$KAFKA_BROKERS" \
        --topic "$KAFKA_TOPIC" \
        --producer.config "$KAFKA_CONFIG"; then
#        send_alert "Kafka publishing failed"
        exit 1
    fi
    # Cleanup - delete only json files and _SUCCESS, keep the directory
    hdfs dfs -rm "$HDFS_DIR"/*.json || true
    hdfs dfs -rm "$HDFS_DIR"/_SUCCESS || true
else
    echo "No files to push to Kafka, skipping."
fi


echo "All queries and processes executed successfully!"

