# PES Backfill Script for all customers in a CDMSDB server

######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define CDMSDB Server Instance Name
$cdmsInstance = "localhost"

### Define Backfill Start Date
$startDate = [DateTime]"2024-09-01"

### Define Backfill End Date
$endDate = [DateTime]"2024-10-08"

### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "na"

### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "E:\xyz_data\dms\pes_backfill\manual"
# Create backfill directory if not exist
New-Item -ItemType Directory -Force -Path $backfillDir

### Define S3 Bucket
$s3Bucket = "esl-ue1-dev01"

### Define S3 Directory
$s3Dir = "q2/esl-service/incoming"

### Define S3 Region (Use "us-east-1" for NA, "eu-west-1" for EMEA, "ap-northeast-1" for Japan)
$s3Region = "us-east-1"

#####################################
##      START BACKFILL PROCESS     ##
#####################################

try {
    # Get list of parent cust_id that is active from xyz_cms_common DB
    $custQuery = "SELECT DISTINCT cust_id FROM t_customer WITH(NOLOCK) WHERE status_id=500 AND parent_cust_id=0"
    $custIds = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query $custQuery | Select-Object -expand cust_id
    if ($custIds.Count -eq 0) {
        Write-Output "No active cust_ids found. Exiting program."
        exit
    }

    # For each active parent cust_id:
    # Step 1 - Get min and max event ID based on date range from event table
    # Step 2 - Run bcp command to generate backfill file
    foreach ($custId in $custIds) {
        $custDbName = "xyz_cms_cust_$custId"
        $eventQuery = @"
            SELECT MIN(camp_id) AS min_event_id, MAX(camp_id) AS max_event_id
            FROM dbo.t_camp_stat WITH(NOLOCK)
            WHERE merge_setup_time BETWEEN '$startDate' AND '$endDate'
                OR dms_setup_time BETWEEN '$startDate' AND '$endDate'
                OR rts_setup_time BETWEEN '$startDate' AND '$endDate'
                OR inb_setup_time BETWEEN '$startDate' AND '$endDate'
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $todayDate = Get-Date -Format "yyyy-MM-dd"
        $todayTime = Get-Date -Format "HHmmss"
        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_campaignPublish_${todayDate}_manual${todayTime}.csv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_launch_camp_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region=$pesRegion"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c '-t,'
    }
} catch {
    Write-Error "Error running ES Backfill: $_"
    exit
}

# Upload files to S3 and delete files that are uploaded successfully
$files = Get-ChildItem -Path $backfillDir -File
foreach ($file in $files) {
    $s3Key = Join-Path $s3Dir $file.Name
    try {
        Write-S3Object -BucketName $s3Bucket -File $file -Key $s3Key -Region $s3Region
        Remove-Item -Path $file -Force
    } catch {
        Write-Error "Error uploading file to S3: $_"
    }
}