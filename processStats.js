const fs = require('fs');
const merge = require('lodash.merge');
const config = require('./config');
const { objectMap, unitToHours } = require('./helpers');

/**
 * Helper function to load current stats from disk
 *
 * @param {string} releaseName  name of current release
 *
 * @return {Object} stats object read from disk
 */
const loadStatsFromDisk = (releaseName) => {
  try {
    return JSON.parse(fs.readFileSync(`${releaseName}/stats.json`, 'utf8'));
  } catch (e) {
    console.log(`error loading stats file: ${e.message}`);
    return {};
  }
};

/**
 * Helper function to load current stats from disk, update, and save back to disk
 *
 * @param {string} releaseName  name of current release
 * @param {Object} newStats     ongoing stats object
 */
const saveStatsToDisk = (releaseName, newStats) => {
  const currentStats = loadStatsFromDisk(releaseName) || {};
  try {
    fs.writeFileSync(
      `${releaseName}/stats.json`,
      JSON.stringify(merge({ ...newStats }, { ...currentStats })),
      'utf8'
    );
  } catch (e) {
    console.log(`error writing stats file: ${e.message}`);
  }
};

/**
 * Helper function to format a row of query data into stats object
 *
 * @param {Object} stats     current stats object
 * @param {Object} row       a db row of clip data
 *
 * @return {Object} updated stats object
 */
const updateClipStats = (stats, row) => {
  // Initialize locale in stats object if it doesn't exist
  const localeStats =
    stats[row.locale] ||
    (stats[row.locale] = {
      clips: 0,
      splits: { accent: {}, age: {}, gender: {} },
      usersSet: new Set(),
    });

  // Increase clips count and unique user count
  localeStats.clips++;
  localeStats.usersSet.add(row.client_id);

  // Update gender/age/accent counts
  const { splits } = localeStats;
  for (const key of Object.keys(splits).filter((key) => key !== 'filter')) {
    const value = row[key] ? row[key] : '';
    splits[key][value] = (splits[key][value] || 0) + 1;
  }

  stats[row.locale] = localeStats;

  return stats;
};

/**
 * Helper function to format stats object after download is complete
 *
 * @param {string} releaseName     name of current release
 * @param {Object} localeSplits    object of interim locale stats with demographic info
 *
 * @return {Object} formatted stats object
 */
const formatFinalClipsStats = (releaseName, localeSplits) => {
  const processedStats = objectMap(
    localeSplits,
    ({ clips, splits, usersSet }) => ({
      clips,

      // convert demographic count sinto demographic ratios
      splits: objectMap(splits, (values) =>
        objectMap(values, (value) => Number((value / clips).toFixed(2)))
      ),

      // convert userSet into user count
      users: usersSet.size,
    })
  );

  saveStatsToDisk(releaseName, { locales: processedStats });
  return processedStats;
};

/**
 * Helper function to generate aggregate stats for final stats object
 *
 * @param {Object} stats     ongoing stats object
 *
 * @return {Object} updated stats object with total durations and clip counts
 */
const calculateAggregateStats = (stats) => {
  let totalDuration = 0;
  let totalValidDurationSecs = 0;

  for (const locale in stats.locales) {
    const localeStats = stats.locales[locale];
    const validClips = localeStats.buckets ? localeStats.buckets.validated : 0;

    localeStats.avgDurationSecs =
      Math.round(localeStats.duration / localeStats.clips) / 1000;
    localeStats.validDurationSecs =
      Math.round((localeStats.duration / localeStats.clips) * validClips) /
      1000;

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
};

/**
 * Helper function to merge stats objects from all stages of bundling and upload
 *
 * @param {Object} stats              ongoing stats object
 * @param {Object} bundlerBucket      datasets bucket object with name and bucket keys
 * @param {string} releaseName       name of current release
 *
 * @return {Object} final formatted stats object
 */
const collectAndUploadStats = async (stats, bundlerBucket, releaseName) => {
  let statsJson;
  const locales = merge(...stats);

  if (config.get('singleBundle')) {
    delete locales.releaseName;
    statsJson = calculateAggregateStats({
      bundleURL: `https://${bundlerBucket.name}.s3.amazonaws.com/${releaseName}/${releaseName}.tar.gz`,
      locales,
      overall: locales[releaseName],
    });
  } else {
    statsJson = calculateAggregateStats({
      bundleURLTemplate: `https://${bundlerBucket.name}.s3.amazonaws.com/${releaseName}/{locale}.tar.gz`,
      locales,
    });
  }

  saveStatsToDisk(releaseName, statsJson);

  return bundlerBucket.bucket
    .putObject({
      Body: JSON.stringify(statsJson),
      Bucket: bundlerBucket.name,
      Key: `${releaseName}/stats.json`,
      ACL: 'public-read',
    })
    .promise();
};

module.exports = {
  updateClipStats,
  saveStatsToDisk,
  formatFinalClipsStats,
  collectAndUploadStats,
  loadStatsFromDisk,
};
