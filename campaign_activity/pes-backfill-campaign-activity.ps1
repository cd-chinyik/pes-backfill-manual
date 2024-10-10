######################################
##      USER DEFINED PARAMETERS     ##
######################################

### Define PES Event Types
$pesEvents = "all"
### Supported PES Event Types: all(all events), send, open, click, unsubscribe, hardBounce, softBounce, webEvent, inbound
### Define CDMSDB Server Instance Name
$cdmsInstance = "localhost"
### Define Backfill Start Date
$startDate = [DateTime]"2024-04-01"
### Define Backfill End Date
$endDate = [DateTime]"2024-10-02"
### Define PES Region (Use "na" for NA custs, "emea" for EMEA custs and "jpn" for Japan custs)
$pesRegion = "na"
### Define PES Backfill Directory Full Path to be stored on the server
$backfillDir = "E:\xyz_data\dms\pes_backfill\manual"
### Define S3 Bucket
$s3Bucket = "pes-cdms-992063009675"

##########################################################################
##      Get list of active parent cust_id from xyz_cms_common DB        ##
##########################################################################      

$custQuery = "SELECT DISTINCT cust_id FROM t_customer WITH(NOLOCK) WHERE status_id=500 AND parent_cust_id=0"
$custIds = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database "xyz_cms_common" -Query $custQuery | Select-Object -expand cust_id
if ($custIds.Count -eq 0) {
    exit
}

##############################################################################################
##      START BACKFILL PROCESS FOR ALL CUSTS                                                ##
##      STEP 1 - Get min/max event IDs for each PES event type                              ##
##      STEP 2 - Run bcp command to generate backfill csv file for each PES event type      ##
##############################################################################################

$todayDate = Get-Date -Format "yyyy-MM-dd"
$todayTime = Get-Date -Format "HHmmss"

# message_send event
if ($pesEvents -like "*send*" -or $pesEvents -like "*all*") {
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
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageSend_${todayDate}_manual${todayTime}.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_send_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c
    }
}

# message_open events
if ($pesEvents -like "*open*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(click_id) AS min_event_id,
                MAX(click_id) AS max_event_id
            FROM dbo.t_click WITH (NOLOCK)
            WHERE click_type_id = 100
                AND click_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageOpen_${todayDate}_manual${todayTime}.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_open_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c
    }
}

# message_click events
if ($pesEvents -like "*click*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(click_id) AS min_event_id,
                MAX(click_id) AS max_event_id
            FROM dbo.t_click WITH (NOLOCK)
            WHERE click_type_id = 200
                AND click_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageClick_${todayDate}_manual${todayTime}.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_click_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c
    }
}

# message_unsubscribe events
if ($pesEvents -like "*unsub*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(unsub_id) AS min_event_id,
                MAX(unsub_id) AS max_event_id
            FROM dbo.t_msg_unsub WITH (NOLOCK)
            WHERE unsub_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageUnsubscribe_${todayDate}_manual${todayTime}.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_unsub_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c
    }
}

# message_hard_bounce events
if ($pesEvents -like "*hardBounce*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
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
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageHardBounce_${todayDate}_manual${todayTime}.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_hard_bounce_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c
    }
}

# message_soft_bounce events
if ($pesEvents -like "*softBounce*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
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
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageSoftBounce_${todayDate}_manual${todayTime}.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_soft_bounce_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c
    }
}

# message_webevent events
if ($pesEvents -like "*webEvent*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(submission_id) AS min_event_id,
                MAX(submission_id) AS max_event_id
            FROM dbo.t_web_event_submission WITH (NOLOCK)
            WHERE event_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageWebEvent_${todayDate}_manual${todayTime}.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_webevent_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c
    }
}

# message_inbound events
if ($pesEvents -like "*inbound*" -or $pesEvents -like "*all*") {
    foreach ($custId in $custIds) {
        $custDbName = "xyz_dms_cust_$custId"
        $eventQuery = @"
            SELECT 
                MIN(response_id) AS min_event_id,
                MAX(response_id) AS max_event_id
            FROM dbo.t_sms_response WITH (NOLOCK)
            WHERE response_time BETWEEN '$startDate' AND '$endDate';
"@
        
        $minMaxResult = Invoke-Sqlcmd -ServerInstance $cdmsInstance -Database $custDbName -Query $eventQuery
        $minEventId = $minMaxResult.min_event_id
        $maxEventId = $minMaxResult.max_event_id

        if ([string]::IsNullOrWhiteSpace($minEventId) -or [string]::IsNullOrWhiteSpace($maxEventId)) {
            continue
        }

        $outputFile = Join-Path $backfillDir "msg-${pesRegion}_${custId}_messageInbound_${todayDate}_manual${todayTime}.tsv"
        $sproc = "EXEC $custDbName.dbo.p_pes_backfill_inbound_get @min_event_id=$minEventId, @max_event_id=$maxEventId, @region='$pesRegion'"
        bcp $sproc QUERYOUT "$outputFile" -S $cdmsInstance -T -k -c
    }
}

###############################################################################
##      UPLOAD ALL BACKFILL FILE TO S3 BUCKET AND DELETE THEM AFTERWARDS     ##
###############################################################################

aws s3 sync "$backfillDir" "s3://$s3Bucket/esl-service/incoming"
Get-ChildItem -Path "$backfillDir" -File | Remove-Item -Force