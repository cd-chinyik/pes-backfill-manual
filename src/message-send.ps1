######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Name
$cdmsInstance = "localhost"
### Define Backfill Start Date
$startDate = [DateTime]"2024-10-01"
### Define Backfill End Date
$endDate = [DateTime]"2024-10-20"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "na"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "E:\xyz_data\dms\pes_backfill\manual"
### Define S3 Bucket
$s3Bucket = "pes-cdms-992063009675"

##########################################################################
##      Get list of active parent cust_id from xyz_cms_common DB        ##
##########################################################################      

$custQuery = "SELECT DISTINCT cust_id FROM t_customer WITH(NOLOCK) WHERE status_id=500 AND parent_cust_id=0"
$custIds = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query $custQuery | Select-Object -expand cust_id
if ($custIds.Count -eq 0) {
    exit
}

######################################################################
##      START BACKFILL PROCESS FOR ALL CUSTS                        ##
##      STEP 1 - Get min/max event IDs                              ##
##      STEP 2 - Run bcp command to generate backfill csv file      ##
######################################################################

foreach ($custId in $custIds) {
    $custDbName = "xyz_dms_cust_$custId"
    $eventQuery = @"
        ;WITH send_cte AS
        (
            SELECT 
                MIN(mc.msg_id) AS min_event_id,
                MAX(mc.msg_id) AS max_event_id
            FROM dbo.t_msg_cold mc WITH (NOLOCK)
            INNER JOIN dbo.t_msg_chunk mck WITH (NOLOCK)
                ON mc.chunk_id = mck.chunk_id
            WHERE mc.chunk_id IS NOT NULL
                AND mck.send_real_time BETWEEN '$startDate' AND '$endDate'
            UNION ALL
            SELECT 
                MIN(mc.msg_id) AS min_event_id,
                MAX(mc.msg_id) AS max_event_id
            FROM dbo.t_msg_cold mc WITH (NOLOCK)
            INNER JOIN dbo.t_msg_instant_trigger_timing mitt WITH (NOLOCK)
                ON mc.msg_id = mitt.msg_id
                AND mc.camp_id = mitt.camp_id
            WHERE mc.chunk_id IS NULL
                AND mitt.send_real_time BETWEEN '$startDate' AND '$endDate'
        )
        SELECT 
            MIN(min_event_id) AS min_event_id,
            MAX(max_event_id) AS max_event_id
        FROM send_cte
"@
    
    $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
    $minEventId = $minMaxResult.min_event_id
    $maxEventId = $minMaxResult.max_event_id

    if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
        continue
    }

    $todayDate = Get-Date -Format "yyyy-MM-dd"
    $todayTime = Get-Date -Format "HHmmss"    
    $fileName = "msg-${pesRegion}_${custId}_messageSend_${todayDate}_pes-backfill-${todayTime}"
    $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
    $sproc = "EXEC $custDbName.dbo.p_pes_backfill_send_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
    bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -w

    $outputUtf8File = Join-Path $backfillDir "${fileName}.tsv"
    Get-Content $outputFile -Encoding Unicode | Set-Content $outputUtf8File -Encoding UTF8
    Remove-Item $outputFile
}

###############################################################################
##      UPLOAD ALL BACKFILL FILE TO S3 BUCKET AND DELETE THEM AFTERWARDS     ##
###############################################################################

# aws s3 sync "$backfillDir" "s3://$s3Bucket/esl-service/incoming"
# Get-ChildItem -Path "$backfillDir" -File | Remove-Item -Force