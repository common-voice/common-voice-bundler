const fs = require('fs');
const path = require('path');

const { countFileLines, promptLoop } = require('./helpers');
const { saveStatsToDisk } = require('./processStats');

/**
 * Main function to wait for user to run corpora-creator separately
 * (It can't be a spawned process because it eats too much memory)
 *
 * @param {string} releaseName   name of current release
 */
const processCorpora = async (releaseName) => {
  const releaseDir = path.join(__dirname, releaseName);
  const tsvPath = path.join(releaseDir, 'clips.tsv');

  const query = `In a separate shell, run the following command:
    create-corpora -f ${tsvPath} -d ${releaseDir} -v\n
When that has completed, return to this shell and type 'corpora-complete' and hit enter > `;

  await promptLoop(query, {
    'corpora-complete': () => {
	return;
    },
  });
};

/**
 * Helper function to create stats object with test/dev/train bucket ocunts
 *
 * @param {array} releaseLocales   array of locale names
 * @param {string} releaseName     name of current release
 *
 * @return {Object} stats object with locale-key and bucket linecount
 */
const countBuckets = async (releaseLocales, releaseName) => {
  const buckets = {};

  for (const locale of releaseLocales) {
    const localePath = path.join(releaseName, locale);

    // Count number of lines in each TSV file for each locale
    const localeBuckets = (await fs.readdirSync(localePath))
      .filter((file) => file.endsWith('.tsv'))
      .map(async (fileName) => [
        fileName,
        Math.max(
          (await countFileLines(path.join(localePath, fileName))) - 1,
          0,
        ),
      ]);

    // Reduce localeBuckets to locale object to match stats formatting
    buckets[locale] = {
      buckets: (await Promise.all(localeBuckets)).reduce(
        (obj, [key, count]) => {
          const newObj = obj;
          newObj[key.split('.tsv')[0]] = count;
          return newObj;
        },
        {},
      ),
    };

    // Load and save stats to disk
    saveStatsToDisk(releaseName, { locales: buckets });
  }

  return buckets;
};

module.exports = {
  countBuckets,
  processCorpora,
};
