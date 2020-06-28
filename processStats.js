const fs = require('fs');
const merge = require('lodash.merge');
const config = require('./config');
const { objectMap, unitToHours } = require('./helpers');

const updateClipStats = (stats, row) => {
  const localeStats =
    stats[row.locale] ||
    (stats[row.locale] = {
      clips: 0,
      splits: { accent: {}, age: {}, gender: {} },
      usersSet: new Set(),
    });

  localeStats.clips++;
  localeStats.usersSet.add(row.client_id);

  const { splits } = localeStats;

  for (const key of Object.keys(splits).filter(key => key != 'filter')) {
    const value = row[key] ? row[key] : '';
    splits[key][value] = (splits[key][value] || 0) + 1;
  }

  stats[row.locale] = localeStats;

  return stats;
};

const formatFinalClipsStats = (releaseName, localeSplits) => {
  const processedStats = objectMap(
    localeSplits,
    ({ clips, splits, usersSet }) => ({
      clips,
      splits: objectMap(splits, values =>
        objectMap(values, value => Number((value / clips).toFixed(2)))
      ),
      users: usersSet.size,
    })
  );

  saveStatsToDisk(releaseName, { locales: processedStats });
  return processedStats;
};

const calculateAggregateStats = (stats, releaseLocales) => {
  let totalDuration = 0;
  let totalValidDurationSecs = 0;

  for (const locale of releaseLocales) {
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

const collectAndUploadStats = async (
  stats,
  releaseLocales,
  bundlerBucket,
  releaseName
) => {
  let statsJson;
  const locales = merge(...stats);

  if (config.get('singleBundle')) {
    statsJson = calculateAggregateStats(
      {
        bundleURL: `https://${bundlerBucket.name}.s3.amazonaws.com/${releaseName}/${releaseName}.tar.gz`,
        locales: { ...locales, [releaseName]: null },
        overall: locales[releaseName],
      },
      releaseLocales
    );
  } else {
    statsJson = calculateAggregateStats(
      {
        bundleURLTemplate: `https://${bundlerBucket.name}.s3.amazonaws.com/${releaseName}/{locale}.tar.gz`,
        locales,
      },
      releaseLocales
    );
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

const saveStatsToDisk = (releaseName, newStats) => {
  const currentStats = loadStatsFromDisk(releaseName) || {};
  try {
    fs.writeFileSync(
      `${releaseName}/stats.json`,
      JSON.stringify(merge(newStats, currentStats)),
      'utf8');
  } catch(e) {
    console.log(`error writing stats file: ${e.message}`);
  }
};

const loadStatsFromDisk = (releaseName) => {
  try {
    return JSON.parse(fs.readFileSync(`${releaseName}/stats.json`, 'utf8'))
  } catch(e) {
    console.log(`error loading stats file: ${e.message}`);
  }
}

module.exports = {
  updateClipStats,
  saveStatsToDisk,
  formatFinalClipsStats,
  collectAndUploadStats,
  loadStatsFromDisk
};
