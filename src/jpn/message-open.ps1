######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Name
$cdmsInstance = "JPXPCDMSDBXX"
### Define Backfill Start Date
$startDate = [DateTime]"2023-01-01"
### Define Backfill End Date
$endDate = [DateTime]"2024-10-20"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "jpn"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "V:\DMS_Data04\pes_backfill\jpn"
### Define batch size
$batchSize = 50000

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
            MIN(click_id) AS min_event_id,
            MAX(click_id) AS max_event_id
        FROM dbo.t_click WITH (NOLOCK)
        WHERE click_type_id = 100
            AND click_time BETWEEN '$startDate' AND '$endDate';
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
        $fileName = "msg-${pesRegion}_${custId}_messageOpen_${todayDate}_pes-backfill-${todayTime}"
        $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_open_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
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

$files = Get-ChildItem -Path $backfillDir
foreach ($file in $files) {
    $filePath = $file.FullName
    $fileName = $file.Name
    $uploadResult = aws s3 cp $filePath "s3://es-loader-an1-prod01/esl-service/incoming/$fileName" --profile "apac_backfill"
    if ($uploadResult -match "upload:") {
        Remove-Item -Path $filePath -Force
    }
}