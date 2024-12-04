######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Names
$cdmsInstance = "NY5PCDMSDB25"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "na"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "V:\DMS_Data04\pes_backfill\na"
### Define batch size
$batchSize = 10000

try {
    Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query "SELECT 1"
} catch {
    Write-Output "Failed to connect to server=${cdmsInstance}"
    continue
}

$custIds = @("1035")

######################################
##      START BACKFILL PROCESS      ##
######################################

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
        $cdmsInstanceFilename = $cdmsInstance.Replace('\', '-')
        $fileName = "msg-${pesRegion}_${custId}_campaignPublish_${todayDate}_${cdmsInstanceFilename}-${todayTime}-batch${batchNum}"
        $outputFile = Join-Path $backfillDir "${filename}-raw.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_launch_camp_get @min_event_id=$batchStart, @max_event_id=$batchEnd, @region=$pesRegion"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -w

        $outputUtf8File = Join-Path $backfillDir "${filename}.tsv"

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
}

###############################################################################
##      UPLOAD ALL BACKFILL FILE TO S3 BUCKET AND DELETE THEM AFTERWARDS     ##
###############################################################################

$files = Get-ChildItem -Path $backfillDir | Where-Object { $_.Name -notlike '*-raw.tsv' }
foreach ($file in $files) {
    $filePath = $file.FullName
    $fileName = $file.Name
    $uploadResult = aws s3 cp $filePath "s3://es-loader-ue1-prod01/esl-service/incoming/$fileName" --profile "na_backfill"
    if ($uploadResult -match "upload:") {
        Remove-Item -Path $filePath -Force
    }
}