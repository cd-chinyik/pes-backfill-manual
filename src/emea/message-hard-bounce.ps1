######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Name
$cdmsInstance = "FF1PCDMSDBXX"
### Define Backfill Start Date
$startDate = [DateTime]"2024-07-01"
### Define Backfill End Date
$endDate = [DateTime]"2024-10-10"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "emea"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "V:\DMS_Data04\pes_backfill\emea"
### Define batch size
$batchSize = 1000000

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
##      STEP 2 - Divide into batches of 1M                          ##
##      STEP 3 - Run bcp command to generate backfill csv file      ##
######################################################################

foreach ($custId in $custIds) {
    $custDbName = "xyz_dms_cust_$custId"
    $eventQuery = @"
        SELECT 
            MIN(b.bounce_id) AS min_event_id,
            MAX(b.bounce_id) AS max_event_id
        FROM dbo.t_msg_bounce b WITH (NOLOCK)
        INNER JOIN dbo.t_bounce_category bc WITH (NOLOCK)
            ON b.category_id = bc.category_id
        WHERE b.bounce_time BETWEEN '$startDate' AND '$endDate'
            AND bc.hard_flag = 1;
"@
    
    $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
    $minEventId = $minMaxResult.min_event_id
    $maxEventId = $minMaxResult.max_event_id

    if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
        continue
    }

    $batchNum = 1
    for ($batchStart = $minEventId; $batchStart -le $maxEventId; $batchStart += $batchSize) {
        $batchEnd = [math]::Min($batchStart + $batchSize - 1, $maxEventId)

        $todayDate = Get-Date -Format "yyyy-MM-dd"
        $todayTime = Get-Date -Format "HHmmss"    
        $fileName = "msg-${pesRegion}_${custId}_messageHardBounce_${todayDate}_pes-backfill-${todayTime}"
        $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_hard_bounce_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -w
    
        $outputUtf8File = Join-Path $backfillDir "${fileName}.tsv"
        Get-Content $outputFile -Encoding Unicode | Set-Content $outputUtf8File -Encoding UTF8
        Remove-Item $outputFile

        $batchNum++
    }
}

###############################################################################
##      UPLOAD ALL BACKFILL FILE TO S3 BUCKET AND DELETE THEM AFTERWARDS     ##
###############################################################################

aws s3 sync "$backfillDir" "s3://es-loader-ew1-prod01/esl-service/incoming" --profile "emea_backfill"
Get-ChildItem -Path "$backfillDir" -File | Remove-Item -Force