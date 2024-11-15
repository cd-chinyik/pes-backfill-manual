######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Names
$cdmsInstances = @(
    "FF1PCDMSDB30"
)
### Define Backfill Start Date
$startDate = [DateTime]"2024-03-04"
### Define Backfill End Date
$endDate = [DateTime]"2024-11-11 16:30:04"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "emea"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "V:\DMS_Data04\pes_backfill\emea"
### Define batch size
$batchSize = 10000

foreach ($cdmsInstance in $cdmsInstances) {
    try {
        Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query "SELECT 1"
    } catch {
        Write-Output "Failed to connect to server=${cdmsInstance}"
        continue
    }

    ######################################################################
    ##      START BACKFILL PROCESS FOR ALL CUSTS                        ##
    ##      STEP 1 - Get min/max event IDs                              ##
    ##      STEP 2 - Divide into batches of 1M                          ##
    ##      STEP 3 - Run bcp command to generate backfill csv file      ##
    ######################################################################

    $custId = "381"
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
    $campIds = @"
107549,107569,106728,107559,107547,110168,107548,107566,107544,107539,107527,107525,107534,107541,107561,107533,107564,107526,107519,107530,
107518,107560,107542,107562,107532,107536,107546,107523,107552,107524,107570,107558
"@
    $campIds = $campIds -replace "`r?`n", ""
    for ($batchStart = $minEventId; $batchStart -le $maxEventId; $batchStart += $batchSize) {
        $batchEnd = [math]::Min($batchStart + $batchSize - 1, $maxEventId)

        $todayDate = Get-Date -Format "yyyy-MM-dd"
        $todayTime = Get-Date -Format "HHmmss"    
        $fileName = "msg-${pesRegion}_${custId}_messageHardBounce_${todayDate}_${cdmsInstance}-${todayTime}"
        $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_hard_bounce_get @min_event_id=$batchStart, @max_event_id=$batchEnd, @region='$pesRegion', @cust_ids='100006', @camp_ids='$campIds'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -w
    
        $outputUtf8File = Join-Path $backfillDir "${fileName}.tsv"

         ### Convert UTF8 No BOM
        $Utf8NoBomEncoding = New-Object System.Text.UTF8Encoding $False
        # Check if file exists
        if (Test-Path $outputFile) {
            $fileContent = Get-Content $outputFile
            
            # Check if fileContent is not an empty array
            if ($fileContent.Count -gt 0) {
                [System.IO.File]::WriteAllLines($outputUtf8File, $fileContent, $Utf8NoBomEncoding)
            } else {
                Write-Output "Skipping Converting to UTF-8 NO BOM because file content is empty."
            }
        } else {
            Write-Output "Skipping Converting to UTF-8 NO BOM because file does not exist."
        }
        Remove-Item $outputFile

        $batchNum++
    }

    ###############################################################################
    ##      UPLOAD ALL BACKFILL FILE TO S3 BUCKET AND DELETE THEM AFTERWARDS     ##
    ###############################################################################
    
    $files = Get-ChildItem -Path $backfillDir | Where-Object { $_.Name -notlike '*-raw.tsv' }
    foreach ($file in $files) {
        $filePath = $file.FullName
        $fileName = $file.Name
        $uploadResult = aws s3 cp $filePath "s3://es-loader-ew1-prod02/esl-service/incoming/$fileName" --profile "emea_backfill"
        if ($uploadResult -match "upload:") {
            Remove-Item -Path $filePath -Force
        }
    }
}