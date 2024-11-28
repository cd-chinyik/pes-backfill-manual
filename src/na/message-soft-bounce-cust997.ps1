######################################
##      USER DEFINED PARAMETERS     ##
######################################

$cdmsInstance = "NY5PCDMSDB26"
$startDate = [DateTime]"2024-02-01"
$endDate = [DateTime]"2024-11-21 17:20:17"
$pesRegion = "na"
$backfillDir = "V:\DMS_Data04\pes_backfill\na"
$batchSize = 10000

try {
    Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query "SELECT 1"
} catch {
    Write-Output "Failed to connect to server=${cdmsInstance}"
    continue
}

######################################
##      START BACKFILL PROCESS      ##
######################################

$custId = "997"
$custDbName = "xyz_dms_cust_$custId"
$eventQuery = @"
    SELECT 
        MIN(b.bounce_id) AS min_event_id,
        MAX(b.bounce_id) AS max_event_id
    FROM dbo.t_msg_bounce b WITH (NOLOCK)
    INNER JOIN dbo.t_bounce_category bc WITH (NOLOCK)
        ON b.category_id = bc.category_id
    WHERE b.bounce_time BETWEEN '$startDate' AND '$endDate'
        AND bc.hard_flag IN (0, 2);
"@

$minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
$minEventId = $minMaxResult.min_event_id
$maxEventId = $minMaxResult.max_event_id

if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
    Write-Output "There is no data to backfill for cust_id=${custId}, server=${cdmsInstance}"
    continue
}

$batchNum = 1
$campIds = "4991,5038,4993,5002,5000,4996,5040,4998,5042"
for ($batchStart = $minEventId; $batchStart -le $maxEventId; $batchStart += $batchSize) {
    $batchEnd = [math]::Min($batchStart + $batchSize - 1, $maxEventId)

    $todayDate = Get-Date -Format "yyyy-MM-dd"
    $todayTime = Get-Date -Format "HHmmss"    
    $fileName = "msg-${pesRegion}_${custId}_messageSoftBounce_${todayDate}_${cdmsInstance}-${todayTime}-batch${batchNum}"
    $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
    $sproc = "EXEC $custDbName.dbo.p_pes_backfill_soft_bounce_get @min_event_id=$batchStart, @max_event_id=$batchEnd, @region='$pesRegion', @camp_ids='$campIds'"
    Write-Output $sproc
    bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -w

    if (-not (Test-Path $outputFile)) {
        Write-Output "Skipping upload: Raw file does not exist."
        continue
    }

    $fileContent = Get-Content $outputFile
    if ($fileContent.Count -le 1) {
        # Early exit if file is empty or contains only headers
        Write-Output "Skipping upload: File is empty or contains only headers."
        Remove-Item $outputFile -Force
        continue
    }

    ### Convert UTF8 No BOM
    $outputUtf8File = Join-Path $backfillDir "${fileName}.tsv"
    $Utf8NoBomEncoding = New-Object System.Text.UTF8Encoding $False
    [System.IO.File]::WriteAllLines($outputUtf8File, $fileContent, $Utf8NoBomEncoding)
    Remove-Item $outputFile -Force

    # Upload directly to S3
    $uploadResult = aws s3 cp $outputUtf8File "s3://es-loader-ue1-prod01/esl-service/incoming/${fileName}.tsv" --profile "na_backfill"
    if ($uploadResult -match "upload:") {
        Remove-Item -Path $outputUtf8File -Force
    } else {
        Write-Output "Failed to upload file: ${fileName}.tsv"
    }

    $batchNum++
}