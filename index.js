require('dotenv').config();
const fs = require('fs');
const path = require('path');
const merge = require('lodash.merge');
const config = require('./config');
const { spawn } = require('promisify-child-process');
const { processAndDownloadClips: _processAndDownloadClips } = require('./getClips');
const { getReportedSentences: _getReportedSentences } = require('./getReportedSentences');
const { countBuckets, processCorpora: _processCorpora } = require('./processCorpora');
const { collectAndUploadStats, saveStatsToDisk } = require('./processStats');
const { uploadDataset: _archiveAndUpload } = require('./upload');

const archiveAndUpload = async (releaseLocales, bundlerBucket, releaseName) => {
  return config.get('skipBundling')
    ? Promise.resolve()
    : _archiveAndUpload(releaseLocales, bundlerBucket, releaseName);
};
const getReportedSentences = async (db, releaseLocales, releaseName) => {
  return config.get('skipReportedSentences')
    ? Promise.resolve()
    : _getReportedSentences(db, releaseLocales, releaseName);
};

const processAndDownloadClips = async (db, clipBucket, releaseName, minorityLangs) => {
  return config.get('startFromCorpora')
    ? Promise.resolve(loadStatsFromDisk(releaseName))
    : _processAndDownloadClips(db, clipBucket, releaseName, minorityLangs);
}

const processCorpora = async releaseName => {
  return config.get('skipCorpora')
    ? Promise.resolve()
    : _processCorpora(releaseName);
};

const checkRuleOfFive = async (db) => {
  const minorityLangs = [];
  const queryFile = path.join(__dirname, 'queries', 'uniqueSpeakers.sql');

  if (config.get('skipMinorityCheck')) return minorityLangs;

  return new Promise(resolve => {
    db.query(fs.readFileSync(queryFile, 'utf-8'))
      .on('result', row => {
        if (row.count < 5 && row.name) minorityLangs.push(row.name);
      })
      .on('end', () => {
        console.log(
          `Languages with fewer than 5 unique speakers: ${minorityLangs.join(
            ', '
          )}`
        );
        resolve(minorityLangs);
      });
  });
};

const sumDurations = async (releaseLocales, releaseName) => {
  const durations = {};
  for (const locale of releaseLocales) {
    const duration = Number(
      (
        await spawn(
          'RUST_BACKTRACE=1 mp3-duration-sum',
          [path.join(releaseName, locale, 'clips')],
          {
            encoding: 'utf8',
            shell: true,
            maxBuffer: 1024 * 1024 * 10,
          }
        )
      ).stdout
    );

    durations[locale] = { duration };
    saveStatsToDisk(releaseName, { locales: durations });
  }

  return durations;
};

const run = () => {
  const RELEASE_NAME = config.get('releaseName');

  const { db, clipBucket, bundlerBucket } = require('./init').initialize();

  db.connect();

  checkRuleOfFive(db)
    .then(minorityLangs =>
      processAndDownloadClips(db, clipBucket, RELEASE_NAME, minorityLangs)
    )
    .then(stats => {
      releaseLocales = Object.keys(stats);

      return Promise.all([
        stats,
        sumDurations(releaseLocales, RELEASE_NAME),
        getReportedSentences(db, releaseLocales, RELEASE_NAME),
        processCorpora(RELEASE_NAME).then(async () => {
          return merge(
            await countBuckets(releaseLocales, RELEASE_NAME),
            await archiveAndUpload(releaseLocales, bundlerBucket, RELEASE_NAME)
          );
        }),
      ]);
    })
    .then(mergedStats => {
      return collectAndUploadStats(
        mergedStats,
        releaseLocales,
        bundlerBucket,
        RELEASE_NAME
      );
    })
    .catch(e => console.error(e))
    .finally(() => {
      db.end();
      process.exit(0);
    });
};

run();
