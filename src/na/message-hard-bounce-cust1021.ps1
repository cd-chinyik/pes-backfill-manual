######################################
##      USER DEFINED PARAMETERS     ##
######################################

$cdmsInstance = "NY5PCDMSDB24"
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

$custId = "1021"
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
    Write-Output "There is no data to backfill for cust_id=${custId}, server=${cdmsInstance}"
    continue
}

$batchNum = 1
$campIds = "84834,23395,57100,37592,49698,60342,63885,50112,42942,46015,50909,89366,43963,47791,46999,36164,52108,66528,8488,18498,12162,32518,83108,48989,6207,7496,40926,34998,43411,68533,27347,33164,36619,31997,21504,23992,5591,11628,56410,26533,77817,44434,44884,20369,63449,80786,15822,30857,33655,75344,45644,60145,28883,28115,37130,25610,57615,35659,79546,38907,73866,39835,58421,54815,46549,41605,34481,10596,39380,90578,65036,54110,64919,41836,14848,22676,13618,58848,9605,48650,34668,38175,8768,40489,103424,183217,6300,6299,183218,15709,219607,129429,127939"
for ($batchStart = $minEventId; $batchStart -le $maxEventId; $batchStart += $batchSize) {
    $batchEnd = [math]::Min($batchStart + $batchSize - 1, $maxEventId)

    $todayDate = Get-Date -Format "yyyy-MM-dd"
    $todayTime = Get-Date -Format "HHmmss"    
    $fileName = "msg-${pesRegion}_${custId}_messageHardBounce_${todayDate}_${cdmsInstance}-${todayTime}-batch${batchNum}"
    $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
    $sproc = "EXEC $custDbName.dbo.p_pes_backfill_hard_bounce_get @min_event_id=$batchStart, @max_event_id=$batchEnd, @region='$pesRegion', @camp_ids='$campIds'"
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