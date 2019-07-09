# archive-upload
Script to compress, archive and upload data files to AWS S3.  The project
that inspired creation of this script was a project to record electric utility
powerhouse data (e.g. voltage, frequency, power) using a Raspberry Pi.  The
data collection script records measurement values in CSV data files and records
application error and other logged information into text log files.  This script
then compresses those files, stores them locally on the Pi in archive directories
and also uploads the files to an AWS S3 bucket.

## Funding

Funding for development of this script was provided by the Alaska Center for 
Energy and Power. 

## Features

Some of the features of this script are:

* The script can watch multiple directories for data files to archive and upload.
  File patterns can be used to identify specific data files in those directories.
* The script can be configured to only archive/upload files that have not been
  modified for a specified amount of time.  For example, if a data collection script
  writes to a data file for each day, the archive script can be told to not archive
  the file until it has not been modified for 30 minutes, for example.
* If the script fails to upload files to AWS S3 due to loss of Internet connectivity
  or any other reason, uploads will be retried each subsequent run of the script until
  successful uploads are achieved.
* The script uses the bz2 compression algorithm, generally providing high levels
  of compression when applied to text data files.  These compressed files are archived
  locally and uploaded to AWS S3.  The original data file is deleted after successfully
  archiving the file on the local system.

## Requirements

This is a Python script requiring Python version 3.6.x or above.  F-strings are
used forcing use of Python 3.6 or above.

Some third party Python packages are required and are identified in the
`requirements.txt` file.  These packages can be installed with the command:

    sudo pip3 install -r requirements.txt

assuming "pip3" is the Python 3 pip command.

The script uses the `boto3` package to upload to AWS S3 buckets. `boto3` expects
to find suitable credentials in the `~/.aws` directory or in environment variables,
as described on [this AWS page.](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html) .  The credentials must allow for writing to the S3 buckets 
that are identified in this script's configuration file.

This script is meant to be run periodically through use of cron or a similar scheduling
tool.

## Usage

Here is an example cron job line to run the script:

    20 * * * * /usr/bin/python3 /home/pi/archive-upload/archive-upload.py /home/pi/archive-config.yaml

The script will run every hour at 20 minutes past the hour.  There is one required command line
argument for the script, which is the path to the script configuration file.  The format of this
configuration file is discussed in the next section.

## Configuration File

A configuration file controls the operation of the script.  The configuration file is
in [YAML format](https://docs.ansible.com/ansible/latest/reference_appendices/YAMLSyntax.html).
Indentation matters in the YAML format, so try to follow the indentation in the sample
configuration file exactly.

Here is a [sample configuration file](https://docs.ansible.com/ansible/latest/reference_appendices/YAMLSyntax.html)
that shows all possible configuration options.  Documentation for each of the
configuration options is provided in this sample file and will not be repeated here.

The configuration file can have any name and can be located anywhere on the
system.  The full path to the configuration file is a required command line argument
for the script.
