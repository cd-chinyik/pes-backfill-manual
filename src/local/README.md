# Powershell Script to Perform PES Backfill for Engage+ Campaign Events

## How it works

- Gets list of active parent cust_id from xyz_cms_common DB in a specified CDMSDB server
- For each active parent cust_id:
    - Get min and max event IDs from respective event tables based on specified date range
    - If either min or max event ID cannot be found from the table, event will <b>not</b> be backfilled
    - Run bcp command based on min and max event IDs obtained and other specified parameters to generate backfill file
- Finally, all the backfill files will be uploaded to specified S3 bucket and all the backfill files will be deleted afterwards

## How to use

- Configure parameters as needed on these lines in the script

| Parameter              | Description                                                               | Example Value                             |
|------------------------|---------------------------------------------------------------------------|-------------------------------------------|
| **`$cdmsInstance`**     | CDMSDB server instance name.                  | `"localhost"`                             |
| **`$startDate`**        | The start date for the backfill operation in 'YYYY-MM-DD' format. (Does not apply to campaign_publish backfill as it pulls all data from the beginning)         | `[DateTime]"2024-09-01"`                  |
| **`$endDate`**          | The end date for the backfill operation in 'YYYY-MM-DD' format. (Does not apply to campaign_publish backfill as it pulls all data from the beginning)           | `[DateTime]"2024-10-08"`                  |
| **`$pesRegion`**        | The PES region. Options: `"na"` for North America, `"emea"` for EMEA, `"jpn"` for Japan. | `"na"`                                    |
| **`$backfillDir`**      | The full path of the directory where the backfill data will be stored.     | `"V:\DMS_Data04\pes_backfill\na"`   |
| **`$s3Bucket`**         | The name of the S3 bucket where the backfill data will be uploaded.        | `"esl-ue1-dev01"`                         |
| **`$s3Profile`**         | AWS Profile Name that points to the correct IAM User credentials stored in `.aws/credential` file locally.        | `"default"`                         |
| **`$batchSize`**         | The number of events to be generated in each backfill file for batching purpose.        | `50000`                         |

- Open Powershell ISE ![alt text](/images/powershell-ise.png)
- Paste the script to run in the text editor and click "Run Script"
- <b>Note</b>: Comment the last 2 lines of each script with `#` to keep the backfill files locally without uploading them to S3.