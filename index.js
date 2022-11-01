require('dotenv').config();
const fs = require('fs');
const path = require('path');
const merge = require('lodash.merge');
const { spawn } = require('promisify-child-process');
const config = require('./config');
const {
  processAndDownloadClips: _processAndDownloadClips,
} = require('./getClips');
const {
  getReportedSentences: _getReportedSentences,
} = require('./getReportedSentences');
const { uploadDataset: _archiveAndUpload } = require('./upload');
const {
  countBuckets,
  processCorpora: _processCorpora,
} = require('./processCorpora');
const {
  collectAndUploadStats,
  saveStatsToDisk,
  loadStatsFromDisk,
} = require('./processStats');
// const { getDuration } = require('./getDuration');
const { db, clipBucket, bundlerBucket } = require('./init').initialize();

/**
 * Check configuration for whether the main function should skip archiving
 * and uploading bundles
 *
 * @param {array} releaseLocales      list of locales in current release
 * @param {Object} bundlerBucket      datasets bucket object with name and bucket keys
 * @param {string} releaseName        name of current release
 *
 * @return {Promise.resolve} either null or updated stats object
 */
const archiveAndUpload = async (releaseLocales, bundlerBucket, releaseName) =>
  config.get('skipBundling')
    ? Promise.resolve()
    : _archiveAndUpload(releaseLocales, bundlerBucket, releaseName);

/**
 * Check configuration for whether the main function should skip downloading
 * reported sentences, used for Singleword release
 *
 * @param {Object} db              MySQL database connection
 * @param {array} releaseLocales   list of locales in current release
 * @param {string} releaseName     name of current release
 *
 * @return {Promise.resolve} either null or updated stats object
 */
const getReportedSentences = async (db, releaseLocales, releaseName) =>
  config.get('skipReportedSentences')
    ? Promise.resolve()
    : _getReportedSentences(db, releaseLocales, releaseName);

/**
 * Check configuration for whether the main function should skip downloading
 * clips, used if current stats file is correct
 *
 * @param {Object} db               MySQL database connection
 * @param {Object} bundlerBucket    clips bucket object with name and bucket keys
 * @param {string} releaseName      name of current release
 * @param {array}  minorityLangs    array of languages with fewer than 5 speakers
 *
 * @return {Promise.resolve} either null or updated stats object
 */
const processAndDownloadClips = async (
  db,
  clipBucket,
  releaseName,
  minorityLangs
) =>
  config.get('startFromCorpora')
    ? Promise.resolve(loadStatsFromDisk(releaseName).locales)
    : _processAndDownloadClips(db, clipBucket, releaseName, minorityLangs);

/**
 * Check configuration for whether the main function should wait
 * for corpora generation
 *
 * @param {string} releaseName      name of current release
 *
 * @return {Promise.resolve} null
 */
const processCorpora = async (releaseName) =>
  config.get('skipCorpora') ? Promise.resolve() : _processCorpora(releaseName);

/**
 * Generate list of languages with fewer than 5 unique speakers who should
 * not have demographic stats generated
 *
 * @param {Object} db              MySQL database connection
 *
 * @return {array} list of locale names
 */
const checkRuleOfFive = async (db) => {
  const minorityLangs = [];
  const queryFile = path.join(__dirname, 'queries', 'uniqueSpeakers.sql');

  if (config.get('skipMinorityCheck')) return minorityLangs;

  return new Promise((resolve) => {
    db.query(fs.readFileSync(queryFile, 'utf-8'))
      .on('result', (row) => {
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

/**
 * Calculate total duration of all mp3s for given locale/releases
 * and writes interim values to disk
 *
 * @param {array}  [releaseLocales]  list of locales
 * @param {string} releaseName       name of current release
 *
 * @return {Object} key-value pairs of locale names + total durations
 */
const sumDurations = async (releaseLocales, releaseName) => {
  const durations = {};

  for (const locale of releaseLocales) {
    const duration = Number(
      (
        await spawn(
          'RUST_BACKTRACE=1 /home/ubuntu/mp3-duration-sum/target/release/mp3-duration-sum',
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

/**
 * Startup function
 *
 * Connects to db, optionally downloads and processes clips, waits
 * for CorporaCreation, and zips and re-uploads clips based on
 * configs
 */
const run = (db, clipBucket, bundlerBucket) => {
  const RELEASE_NAME = config.get('releaseName');
  console.log(`Starting Release: ${RELEASE_NAME}`);
  db.connect();
  console.log('Connected to database');
  // Check for minorit languages
  checkRuleOfFive(db)
    // Download clips, create TSV object
    .then((minorityLangs) =>
      processAndDownloadClips(db, clipBucket, RELEASE_NAME, minorityLangs)
    )
    .then((stats) => {
      const releaseLocales = Object.keys(stats);

      // wait for all processes to finish
      return Promise.all([
        stats,
        sumDurations(releaseLocales, RELEASE_NAME),
        getReportedSentences(db, releaseLocales, RELEASE_NAME),
        processCorpora(RELEASE_NAME).then(async () =>
          merge(
            // merge test/dev/train bucket stats with archive and upload stats
            await countBuckets(releaseLocales, RELEASE_NAME),
            await archiveAndUpload(releaseLocales, bundlerBucket, RELEASE_NAME)
          )
        ),
      ]);
    })
    .then((mergedStats) =>
      collectAndUploadStats(
        // process and upload stats file
        mergedStats,
        bundlerBucket,
        RELEASE_NAME
      )
    )
    .catch((e) => console.error(e))
    .finally(() => {
      // close db
      db.end();
      process.exit(0);
    });
};

run(db, clipBucket, bundlerBucket);
