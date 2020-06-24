const fs = require('fs');
const path = require('path');

const { countFileLines, promptLoop } = require('./helpers');

const processCorpora = async releaseName => {
  const releaseDir = path.join(__dirname, releaseName);
  const tsvPath = path.join(releaseDir, 'clips.tsv');

  const query = `In a separate shell in the same directory:
    create-corpora -f ${tsvPath} -d ${releaseDir} -v\n
When that has completed, return to this shell and type 'corpora-complete' and hit enter > `;

  await promptLoop(query, {
    'corpora-complete': () => {
      return;
    },
  });
};

const countBuckets = async (releaseLocales, releaseName) => {
  const buckets = {};

  for (const locale of releaseLocales) {
    const localePath = path.join(releaseName, locale);
    const localeBuckets = (await fs.readdirSync(localePath))
      .filter(file => file.endsWith('.tsv'))
      .map(async fileName => [
        fileName,
        Math.max(
          (await countFileLines(path.join(localePath, fileName))) - 1,
          0
        ),
      ]);

    buckets[locale] = {
      buckets: (await Promise.all(localeBuckets)).reduce(
        (obj, [key, count]) => {
          obj[key.split('.tsv')[0]] = count;
          return obj;
        },
        {}
      ),
    };
  }

  return buckets;
};

module.exports = {
  countBuckets,
  processCorpora,
};
