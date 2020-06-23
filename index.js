require('dotenv').config();
const fs = require('fs');
const path = require('path');
const merge = require('lodash.merge');
const config = require('./config');
const { getReportedSentences } = require('./getReportedSentences');

const {
  countFileLines,
  promptLoop,
  unitToHours,
  getLocaleDirs,
  sumDurations
} = require('./helpers');

const RELEASE_NAME = config.get('releaseName');
const TSV_PATH = path.join(RELEASE_NAME, 'clips.tsv');
const { name: OUT_BUCKET_NAME } = config.get('outBucket');

let localeDirs = [];

const { db, clipBucket, bundlerBucket } = require('./init').initialize();
const { processAndDownloadClips } = require('./getClips');
const { uploadDataset } = require('./managedUpload');

const runCorpora = async () => {
  const query = `In a separate shell in the same directory, run the following command:
    create-corpora -f ${TSV_PATH} -d ${RELEASE_NAME} -v\n
When that has completed, return to this shell and type 'corpora-complete' and hit enter > `

  await promptLoop(query, {
    'corpora-complete': () => { return; }
  });

  const buckets = {};

  for (const locale of localeDirs) {
    const localePath = path.join(RELEASE_NAME, locale);
    const localeBuckets = (await fs.readdirSync(localePath))
      .filter(file => file.endsWith('.tsv'))
      .map(async fileName => [
        fileName,
        Math.max((await countFileLines(path.join(localePath, fileName))) - 1, 0)
      ]);

    buckets[locale] = {
      buckets: (await Promise.all(localeBuckets)).reduce(
        (obj, [key, count]) => {
          obj[key.split('.tsv')[0]] = count;
          return obj;
        },
        {}
      )
    };
  }

  return buckets;
};

const calculateAggregateStats = stats => {
  let totalDuration = 0;
  let totalValidDurationSecs = 0;

  for (const locale in stats.locales) {
    const localeStats = stats.locales[locale];
    const validClips = localeStats.buckets ? localeStats.buckets.validated : 0;

    localeStats.avgDurationSecs = Math.round((localeStats.duration / localeStats.clips)) / 1000;
    localeStats.validDurationSecs = Math.round((localeStats.duration / localeStats.clips) * validClips) / 1000;

    localeStats.totalHrs = unitToHours(localeStats.duration, 'ms', 2);
    localeStats.validHrs = unitToHours(localeStats.validDurationSecs, 's', 2);

    stats.locales[locale] = localeStats;

    totalDuration += localeStats.duration;
    totalValidDurationSecs += localeStats.validDurationSecs;
  }

  stats.totalDuration = Math.floor(totalDuration);
  stats.totalValidDurationSecs = Math.floor(totalValidDurationSecs);
  stats.totalHrs = unitToHours(stats.totalDuration, 'ms', 0);
  stats.totalValidHrs = unitToHours(stats.totalValidDurationSecs, 's', 0);

  return stats;
}

const collectAndUploadStats = async stats => {
  const statsJSON = calculateAggregateStats({
    bundleURLTemplate: `https://${OUT_BUCKET_NAME}.s3.amazonaws.com/${RELEASE_NAME}/{locale}.tar.gz`,
    locales: merge(...stats)
  });

  saveStatsToDisk(statsJSON);

  return bundlerBucket
    .putObject({
      Body: JSON.stringify(statsJSON),
      Bucket: OUT_BUCKET_NAME,
      Key: `${RELEASE_NAME}/stats.json`,
      ACL: 'public-read'
    })
    .promise();
};

const saveStatsToDisk = stats => {
  fs.writeFile(`${RELEASE_NAME}/stats.json`, JSON.stringify(stats), 'utf8', (err) => {
    if (err) throw err;
  });
}

const archiveAndUpload = async () => {
  return config.get('skipBundling') ? Promise.resolve() : uploadDataset(localeDirs, bundlerBucket, RELEASE_NAME);
}

const countBuckets = async () => {
  return config.get('skipCorpora') ? Promise.resolve() : runCorpora();
}

const downloadReportedSentences = async(db, localeDirs, releaseName) => {
  return config.get('skipReportedSentences') ? Promise.resolve() : getReportedSentences(db, localeDirs, releaseName);
}

const checkRuleOfFive = async () => {
  const minorityLangs = [];
  const queryFile = path.join(__dirname, 'queries', 'uniqueSpeakers.sql');

  if (config.get('skipMinorityCheck')) return minorityLangs;

  return new Promise(resolve => {
    db.query(fs.readFileSync(queryFile, 'utf-8'))
    .on('result', row => {
      if (row.count < 5 && row.name) minorityLangs.push(row.name);
    })
    .on('end', () => {
      console.log(`Languages with fewer than 5 unique speakers: ${minorityLangs.join(", ")}`);
      resolve(minorityLangs);
    });
  });
}

const run = () => {
  db.connect();

  checkRuleOfFive()
    .then(minorityLangs =>
      processAndDownloadClips(db, clipBucket, minorityLangs))
    .then(stats => {
      saveStatsToDisk(stats);
      localeDirs = getLocaleDirs(RELEASE_NAME);

      return Promise.all([
        stats,
        sumDurations(localeDirs, RELEASE_NAME),
        downloadReportedSentences(db, localeDirs, RELEASE_NAME),
        countBuckets().then(async bucketStats =>
          merge(
            bucketStats,
            await archiveAndUpload(localeDirs, bundlerBucket, RELEASE_NAME)
          )
        )
      ]);
    })
    .then(collectAndUploadStats)
    .catch(e => console.error(e))
    .finally(() => {
      db.end();
      process.exit(0)
    });
}

run();