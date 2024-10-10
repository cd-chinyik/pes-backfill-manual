# Powershell Script to Perform PES Backfill for Engage+ Campaign Activity Events

## How it works

- Gets list of active parent cust_id from xyz_cms_common DB in a specified CDMSDB server
- For each active parent cust_id:
    - Get min and max event IDs from respective event table based on specified date range
    - If either min or max event ID cannot be found from the table, event will <b>not</b> be backfilled
    - Run bcp command based on min and max event IDs obtained and other specified parameters to generate backfill file
- Finally, all the backfill files will be uploaded to specified S3 bucket

## How to use

- Configure parameters as needed on these lines in the script

| Parameter              | Description                                                               | Example Value                             |
|------------------------|---------------------------------------------------------------------------|-------------------------------------------|
| **`$pesEvents`**     | Engage+ Campaign Activity Events to perform backfill, comma separated.                  | `"all"`, `"launch,send"` 
| **`$cdmsInstance`**     | The name or address of the CDMS database server instance.                  | `"localhost"`                             |
| **`$startDate`**        | The start date for the backfill operation (in `DateTime` format).          | `[DateTime]"2024-09-01"`                  |
| **`$endDate`**          | The end date for the backfill operation (in `DateTime` format).            | `[DateTime]"2024-10-08"`                  |
| **`$pesRegion`**        | The PES region. Options: `"na"` for North America, `"emea"` for EMEA, `"jpn"` for Japan. | `"na"`                                    |
| **`$backfillDir`**      | The full path of the directory where the backfill data will be stored.     | `"E:\xyz_data\dms\pes_backfill\manual"`   |
| **`$s3Bucket`**         | The name of the S3 bucket where the backfill data will be uploaded.        | `"esl-ue1-dev01"`                         |
| **`$s3Dir`**            | The directory path within the S3 bucket where the data will be stored.     | `"q2/esl-service/incoming"`               |
| **`$s3Region`**         | The AWS region of the S3 bucket. Options: `"us-east-1"`, `"eu-west-1"`, `"ap-northeast-1"`. | `"us-east-1"`                             |
- Run the script

### Additional Details:
- The backfill directory is created if it does not already exist.
- Make sure the `$s3Bucket` and `$s3Region` match the configuration of your S3 bucket for the backfill upload process.
- Supported Campaign Activity type string (case-insensitive):
    - "all"
    - "launch"
    - "send"
    - "open"
    - "click"
    - "unsub"
    - "hardBounce"
    - "softBounce"
    - "webEvent"
    - "inbound"