# CommonVoice Bundler

Script for bundling Common Voice (https://voice.mozilla.org) clips by language.

## What it does

1. Query database for all clip data
1. Download all those clips from an S3
1. Anonymize clips `client_id` and filename (called `path`)
1. Upload a tsv file with all the anonymized clip data
1. Put clips into archives by language and upload it to (a different) S3
1. Calculate statistics for all the languages

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
