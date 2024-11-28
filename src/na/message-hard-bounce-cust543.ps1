######################################
##      USER DEFINED PARAMETERS     ##
######################################

$cdmsInstance = "NY5PCDMSDB04"
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

$custId = "543"
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
$campIds = "5611,8183,17067,17090,17091,17092,17093,17094,17095,17096,17098,17099,17100,17101,17102,17103,17104,17139,17140,17274,17541,17542,17543,17813,17814,17817,17820,18279,19030,19032,19033,19237,23161,23231,25897,26297,26298,27808,28522,29602,31943,32185,32607,34894,39021,40159,43134,46532,47073,55102,56444,56533,56538,57211,61589,63080,67044,67083,67084,67593,71850,71851,73966,74288,77608,85811,87097,91565,91566,91567,93470,93723,97288,105412,112531,112831,112832,115863,116378,116752,119025,119072,119463,122545,123491,124031,124036,124038,128643,129730,129923,129927,130127,132076,134019,135746,137259,137260,141845,143045,143054,144160,145227,145351,145353,145654,145910,146231,147279,149523,153520,153521,153641,153642,155306,155728,155746,155958,157684,158524,158525,165368,169371,170307,172321,172346,173779,182180,188877,188881,188883,188886,191683,191744,193680,197723,199754,199756,199807,204677,204694,204702,210167,210245,210246,210259,210328,210330,210520,213821,214776,224835,224837,227450,229856,229869,229881,230021,230640,231904,233083,239396,240062,240094,241000,241157,241279,244299,244593,246184,247816,248530,249033,254333,258009,258010,258442,258901,258902,258905,258916,258925,258933,259985,260624,262998,271192,274573,279883,282742,283608,286042,286071,287948,287953,287956,287980,288027,289569,289886,297237,298967,298968,300445,302649,305496,305505,305506,305508,305755,307429,307712,309953,310523,314437,315553,315554,315555,315598,315771,315775,318517,318914,318939,320260,321351,323043,325717,326489,326504,326597,327470,329957,331480,332202,334310,334368,334475,341285,341286,341287,341288,341291,341292,341293,341294,341310,341311,341312,341313,341326,341327,341328,341329,341609,342291,342293,342299,342331,342335,344162,344163,344523,345027,345435,345938,345941,346878,346879,347051,347169,347562,347563,350826,351586,355159,356950,358086,365603,366201,366521,367067,367068,367069,367070,367071,367936,370660,371928,371929,371930,371931,371932,371933,371934,376500,377083,379900,380302,381715,383989,384182,387569,387588,388505,389725,390472,391090,391241,392239,392309,392399,393135,393755,393852,396866,396906,397612,398612,402679,402924,404746,404749,404750,405553,406082,406084,406086,406816,407987,408637,409727,410034,410449,411516,411517,411518,411519,411525,411526,411527,411528,412725,413363,413375,413384,414431,414440,414452,415455,416433,417008,418344,419789,419790,419792,419793,420935,422937,423802,425514,426045,426691,426692,426963,426967,427369,427818,428097,428733,428820,428919,429579,429614,430287,430288,430615,431453,431454,431617,432452,433294,441989,442376,442671"
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