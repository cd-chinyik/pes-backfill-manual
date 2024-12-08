$pesRegion = "na"
$backfillDir = "V:\DMS_Data04\pes_backfill\na"
$batchSize = 10000
$backfillTargets = @{
    "NY5PCDMSDB02" = @(1138)
    "NY5PCDMSDB04" = @(543)
    "NY5PCDMSDB11" = @(1282)
    "NY5PCDMSDB13" = @(445,1158,1166,1179,1180,1184,1193,1204)
    "NY5PCDMSDB16" = @(1205)
    "NY5PCDMSDB17" = @(1098)
    "NY5PCDMSDB23" = @(722,766)
    "NY5PCDMSDB25" = @(1035)
    "NY5PCDMSDB34" = @(1172)
}

foreach ($cdmsInstance in $backfillTargets.Keys) {
    try {
        Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query "SELECT 1"
    } catch {
        Write-Output "Failed to connect to server=${cdmsInstance}"
        continue
    }

    $custIds = $backfillTargets[$cdmsInstance]
    
    foreach ($custId in $custIds) {
        try{
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
        catch {
            Write-Output "Backfill failed for CDMS=$cdmsInstance, cust_id=$custId"
            continue
        }
    }
}

$files = Get-ChildItem -Path $backfillDir | Where-Object { $_.Name -notlike '*-raw.tsv' }
foreach ($file in $files) {
    $filePath = $file.FullName
    $fileName = $file.Name
    $uploadResult = aws s3 cp $filePath "s3://es-loader-ue1-prod01/esl-service/incoming/$fileName" --profile "na_backfill"
    if ($uploadResult -match "upload:") {
        Remove-Item -Path $filePath -Force
    }
}