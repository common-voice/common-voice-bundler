# CommonVoice Bundler

Script for bundling Common Voice (https://voice.mozilla.org) clips by language.

## What it does

1. Query database for all clip data
1. Download all those clips from an S3, separated into language directories
1. Write the clips metadata to a `clips.tsv` file with anonymized `client_id` values
1. Analyze the clips metadata and assemble aggregate stats for `stats.json`
1. Calculate the total duration of each dataset
1. Prompt you to run `corpora-creator`, which will take the `clips.tsv` file and analyze it to create test/dev/train sets for machine learning purposes
1. Create `.tar.gz` bundles according to your settings, usually one per language
1. Create a checksum for each tarball
1. Upload the tarball to S3
1. Write the checksum to the `stats.json` file and also upload that to S3

## How to run it

1. Install [node](https://nodejs.org) (>= 8.3.0)
1. Install [yarn](https://yarnpkg.com/docs/install)
1. Install [CorporaCreator](https://github.com/mozilla/CorporaCreator)
1. Install [mp3-duration-sum](https://github.com/Gregoor/mp3-duration-sum)
1. `git clone git@github.com:Common-Voice/common-voice-bundler.git`
1. Override the keys defined in [config.js](https://github.com/Common-Voice/common-voice-bundler/blob/master/config.js) with a `config.json` in the same dir
1. `yarn`
1. `yarn start`
1. You will be prompted to run `corpora-creator` separately. Follow the instructions.

## Configuration

In order to run this, you need to override the default keys defined in [config.js](https://github.com/Common-Voice/common-voice-bundler/blob/master/config.js) with a `config.json` in the same directory. At an absolute minimum, you will need: 

* `releaseName`: the name of the release. this can take the form of an AWS key, and `/` in the name will be treated as directories
* `queryFile`: the name of the file that specifies the SQL query for a given dataset - see `/queries` directory for past files
* the `db` object
* the `clipBucket` object
* the `outBucket` object (which refers to the bucket that the bundled dataset will be hosted on)

The other options are:

* `cutoffTime`: clips will only be downloaded if they were created before this time
* `startCutoffTime`: clips will only be included if they were created *after* this time. To be used for delta releases and inconjuction with `cutoffTime`
* `skipBundling`: this will do everything except bundle and upload clips (used mostly for testing)
* `skipCorpora`: this will do everything but skip waiting for you to create the corpora (used if the process was interrupted and you already have the appropriate corpora)
* `skipHashing`: this will skip hashing the client ID (used mostly for testing)
* `skipDownload`: this will skip downloading the file and just create the `clips.tsv` (used mostly for testing)
* `skipMinorityCheck`: this will skip checking which languages have fewer than 5 speakers
* `skipReportedSentences`: this will not include the list of reported sentences in each dataset (used for the singleword target segment bundle)
* `startFromCorpora`: this will begin the whole process at the prompt for the corpora (used if the process was interrupted and you already have all the files and clips metadata)
* `singleBundle`: this will create a single archive with all languages, instead of one tar per language

## Resume from interruptions

You should run this script from a `tmux` in the EC2 shell you're provided with, so that if your connection dies the script can still continue to run. Sometimes, the script itself will die, in which case it will attempt to gracefully recover in the following ways:

* It will skip downloading files that are already on disk
* It will skip tarring/uploading language bundles that have already been successfully uploaded
* It will attempt to write to and load from `stats.json` as much as possible, so that you have in-progress stats even if the whole process doesn't finish

In addition, you can use the options specified above to resume from key points in the process instead of running through the entire process from scratch. 

## Troubleshooting

* **`stats.json` has durations of 0 for some/all languages**: `mp3-duration-sum` runs in the background after all the clips have been downloaded, and there is no signal when it completes other than the stats file receiving updated durations. If you skip corpora creation or if most of your tar files have already been created, the script my terminate before `mp3-duration-sum` has completed and updated the stats file. The work around for this is to artificially pause the script by setting `skipCorpora` as false, and simply not moving onto the next stage until you've verified that the durations have been updated
* **`CorporaCreator` terminates or runs out of memory**: The Corpora Creator is itself somewhat fragile, as it hasn't been substantially updated since it was created, and may need tweaking to run. You can test where the bug is by creating a smaller version of `clips.tsv` by taking the first 10,000 rows using `head` and then trying to run `CorporaCreator` on the smaller file, to identify whether the bug is the file size or your install. If the problem is the file size, you may need to upgrade to a larger instance of EC2. Contact IT-SE. 
