######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Names
$cdmsInstances = @(
    "FF1PCDMSDB31"
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

    $custId = "1166"
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
    $campIds = @"
1582,1088,879,1916,1262,1710,793,1750,1143,438,2473,787,1151,2432,308,439,2253,1902,2132,1352,1627,2174,2422,2234,108,2496,1404,325,365,761,
2447,938,2328,301,105,1743,2478,2490,1770,1779,520,1082,2247,1059,584,1572,997,858,1709,2599,2337,355,513,1338,346,802,957,1194,873,2407,2052,
1532,1630,939,106,1454,361,2460,829,2207,1341,347,2088,1333,2544,1506,1345,1181,1555,578,1026,1051,797,820,2176,2383,1478,1665,2298,966,2491,
2039,1199,863,1918,2206,2038,2608,1121,2301,734,833,892,2356,2299,1466,1900,2379,1134,1195,1476,637,291,2029,590,2110,1106,615,2185,2021,2147,
1021,2006,2348,103,1279,1334,307,1628,327,1156,1391,1418,1649,991,309,2037,2520,962,435,1901,1291,316,2018,1999,1574,302,834,306,396,909,2007,
1455,2079,437,884,731,2346,2325,2226,1205,2096,110,498,533,96,294,2158,413,1302,544,2300,1456,2359,1583,1915,104,1317,506,877,2588,1718,1371,
2398,571,1096,1237,684,1507,304,2074,2607,2131,1075,1526,2395,1432,2102,756,339,2271,297,2022,2419,2374,315,1535,2548,2028,344,2509,1629,1382,
709,2237,2408,602,107,2283,305,1749,817,1426,601,1638,1422,1836,310,311,318,2286,2435,2446,620,1773,300,1502,2217,165,2610,252,1666,1098,911,
1711,955,2148,1201,2195,436,332,2159,916,1559,1203,99,1254,1783,432,853,529,94,319,633,1131,803,335,2157,1845,2139,312,1030,1022,847,828,2053,
1857,570,1209,2409,1077,1123,703,504,2474,743,1070,1968,2199,2129,440,519,95,1642,2459,2609,1434,1296,2280,2458,2368,303,1390,993,1058,404,2124,
2012,2332,1267,1664,547,1728,1731,1762,1280,101,882,2104,2106,811,2183,348,2330,98,2347,531,317,2060,1886,989,1982,2524,1278,2315,364,2367,2463,
1685,1336,594,1978,109,707,1852,2241,685,1182,708,2068,2141,100,951,2087,795,2602,2384,515,97,2248,1584,393,2173,1239,102,369,2472,1990,1102,
2543,1020,2559,441,342,314,422,1292,521,407,1519,1505,1337,1265,2427,434,419,2547
"@
    $campIds = $campIds -replace "`r?`n", ""
    for ($batchStart = $minEventId; $batchStart -le $maxEventId; $batchStart += $batchSize) {
        $batchEnd = [math]::Min($batchStart + $batchSize - 1, $maxEventId)

        $todayDate = Get-Date -Format "yyyy-MM-dd"
        $todayTime = Get-Date -Format "HHmmss"    
        $fileName = "msg-${pesRegion}_${custId}_messageSoftBounce_${todayDate}_${cdmsInstance}-${todayTime}-batch${batchNum}"
        $outputFile = Join-Path $backfillDir "${fileName}-raw.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_soft_bounce_get @min_event_id=$batchStart, @max_event_id=$batchEnd, @region='$pesRegion', @camp_ids='$campIds'"
        Write-Output $sproc
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