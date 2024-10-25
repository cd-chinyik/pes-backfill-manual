######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Names
$cdmsInstances = @(
    "localhost", "localhost2"
)
### Define Backfill Start Date
$startDate = [DateTime]"2024-10-01"
### Define Backfill End Date
$endDate = [DateTime]"2024-10-20"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "na"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "E:\xyz_data\dms\pes_backfill\manual"
### Define batch size
$batchSize = 10000

foreach ($cdmsInstance in $cdmsInstances) {
    try {
        Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query "SELECT 1"
    } catch {
        Write-Output "Failed to connect to server=${cdmsInstance}"
        continue
    }

    ##########################################################################
    ##      Get list of active parent cust_id from xyz_cms_common DB        ##
    ##########################################################################      

    $custQuery = "SELECT DISTINCT cust_id FROM t_customer WITH(NOLOCK) WHERE status_id=500 AND parent_cust_id=0"
    $custIds = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query $custQuery | Select-Object -expand cust_id
    if ($custIds.Count -eq 0) {
        Write-Output "There is no active parent cust_id in server=${cdmsInstance}"
        continue
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
            Write-Output "There is no data to backfill for cust_id=${custId}, server=${cdmsInstance}"
            continue
        }

        $batchNum = 1
        for ($batchStart = $minEventId; $batchStart -le $maxEventId; $batchStart += $batchSize) {
            $batchEnd = [math]::Min($batchStart + $batchSize - 1, $maxEventId)

            $todayDate = Get-Date -Format "yyyy-MM-dd"
            $todayTime = Get-Date -Format "HHmmss"    
            $fileName = "msg-${pesRegion}_${custId}_messageSend_${todayDate}_${cdmsInstance}-${todayTime}"
            $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
            $sproc = "EXEC $custDbName.dbo.p_pes_backfill_send_get @min_event_id=$batchStart, @max_event_id=$batchEnd, @region='$pesRegion'"
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
        $uploadResult = aws s3 cp $filePath "s3://esl-ue1-dev01/q1/esl-service/incoming/$fileName"
        if ($uploadResult -match "upload:") {
            Remove-Item -Path $filePath -Force
        }
    }
}