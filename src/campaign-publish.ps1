######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Name
$cdmsInstance = "localhost"
### Define Backfill Start Date
$startDate = [DateTime]"2022-10-17"
### Define Backfill End Date
$endDate = [DateTime]"2024-10-19"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "na"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "E:\xyz_data\dms\pes_backfill\manual"
### Define S3 Bucket
$s3Bucket = "pes-cdms-992063009675"

##########################################################################
##      Get list of active parent cust_id from xyz_cms_common DB        ##
##########################################################################  

# Get list of parent cust_id that is active from xyz_cms_common DB
$custQuery = "SELECT DISTINCT cust_id FROM t_customer WITH(NOLOCK) WHERE status_id=500 AND parent_cust_id=0"
$custIds = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query $custQuery | Select-Object -expand cust_id
if ($custIds.Count -eq 0) {
    Write-Output "No active cust_ids found. Exiting program."
    exit
}

######################################################################
##      START BACKFILL PROCESS FOR ALL CUSTS                        ##
##      STEP 1 - Get min/max event IDs                              ##
##      STEP 2 - Run bcp command to generate backfill csv file      ##
######################################################################

foreach ($custId in $custIds) {
    $custDbName = "xyz_cms_cust_$custId"
    $eventQuery = @"
        SELECT MIN(camp_id) AS min_event_id, MAX(camp_id) AS max_event_id
        FROM dbo.t_camp_stat WITH(NOLOCK)
        WHERE merge_setup_time BETWEEN '$startDate' AND '$endDate'
            OR dms_setup_time BETWEEN '$startDate' AND '$endDate'
            OR rts_setup_time BETWEEN '$startDate' AND '$endDate'
            OR inb_setup_time BETWEEN '$startDate' AND '$endDate'
"@
    
    $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
    $minEventId = $minMaxResult.min_event_id
    $maxEventId = $minMaxResult.max_event_id

    if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
        continue
    }

    $todayDate = Get-Date -Format "yyyy-MM-dd"
    $todayTime = Get-Date -Format "HHmmss"
    $fileName = "msg-${pesRegion}_${custId}_campaignPublish_${todayDate}_pes-backfill-${todayTime}"
    $outputFile = Join-Path $backfillDir "${filename}-raw.tsv"
    $sproc = "EXEC $custDbName.dbo.p_pes_backfill_launch_camp_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region=$pesRegion"
    bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -w

    $outputUtf8File = Join-Path $backfillDir "${filename}.tsv"
    Get-Content $outputFile -Encoding Unicode | Set-Content $outputUtf8File -Encoding UTF8
    Remove-Item $outputFile
}

###############################################################################
##      UPLOAD ALL BACKFILL FILE TO S3 BUCKET AND DELETE THEM AFTERWARDS     ##
###############################################################################

aws s3 sync "$backfillDir" "s3://$s3Bucket/q1/esl-service/incoming"
Get-ChildItem -Path "$backfillDir" -File | Remove-Item -Force