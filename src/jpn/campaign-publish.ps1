######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Names
$cdmsInstances = @(
    "JPXPCDMSDBXX", "JPXPCDMSDBXX", "JPXPCDMSDBXX", "JPXPCDMSDBXX", "JPXPCDMSDBXX", 
    "JPXPCDMSDBXX", "JPXPCDMSDBXX", "JPXPCDMSDBXX", "JPXPCDMSDBXX", "JPXPCDMSDBXX"
)
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "jpn"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "V:\DMS_Data04\pes_backfill\jpn"
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
        $custDbName = "xyz_cms_cust_$custId"
        $eventQuery = @"
            SELECT MAX(camp_id) AS max_event_id
            FROM dbo.t_camp_stat WITH(NOLOCK)
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = 1
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($maxEventId)) {
            Write-Output "There is no data to backfill for cust_id=${custId}, server=${cdmsInstance}"
            continue
        }

        $batchNum = 1
        for ($batchStart = $minEventId; $batchStart -le $maxEventId; $batchStart += $batchSize) {
            $batchEnd = [math]::Min($batchStart + $batchSize - 1, $maxEventId)

            $todayDate = Get-Date -Format "yyyy-MM-dd"
            $todayTime = Get-Date -Format "HHmmss"
            $fileName = "msg-${pesRegion}_${custId}_campaignPublish_${todayDate}_${cdmsInstance}-${todayTime}-batch${batchNum}"
            $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
            $sproc = "EXEC $custDbName.dbo.p_pes_backfill_launch_camp_get @min_event_id=$batchStart, @max_event_id=$batchEnd, @region=$pesRegion"
            bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -w

            $outputUtf8File = Join-Path $backfillDir "${fileName}.tsv"

 	        ### Convert UTF8 No BOM
            $Utf8NoBomEncoding = New-Object System.Text.UTF8Encoding $False
            [System.IO.File]::WriteAllLines($outputUtf8File , (Get-Content $outputFile), $Utf8NoBomEncoding)
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
}