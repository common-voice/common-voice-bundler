const fs = require('fs');
const path = require('path');

const config = require('./config');
const { connectToDb } = require('./init');
const { loadStatsFromDisk } = require('./processStats');

const db = connectToDb();
const getTimesPathFromLocale = (locale) =>
  path.join(__dirname, RELEASE_NAME, locale, 'times.txt');
const getFileContents = (path) => fs.readFileSync(path, 'utf-8');

const RELEASE_NAME = config.get('releaseName');
const BATCH_SIZE = 5000;

const extractIdFromFilePathString = (filepath) => {
  return +filepath
    .split('_')
    .slice(-1)
    .flatMap((x) => x.split('.'))
    .slice(0, 1)
    .reduce((_, y) => y);
};

const getIdAndDuration = (timesLine) => {
  const [filePathWithId, duration] = timesLine.split('\t');
  const id = extractIdFromFilePathString(filePathWithId);
  return { id, duration: +duration };
};

const getIdsAndDurationFromLocale = (locale) => {
  return getFileContents(getTimesPathFromLocale(locale))
    .split('\n')
    .filter((s) => s !== '')
    .map(getIdAndDuration);
};

const buildValues = (id, duration) => {
  return `(${id}, client_id, path, sentence, original_sentence_id, ${duration})`;
};

const batchUpdateClipsTable = async (idsAndDurations) => {
  const values = idsAndDurations.map(({ id, duration }) => buildValues(id, duration));
  const total = values.length;

  let current = 0;
  let start = 0;

  while (true) {
    const end = start + BATCH_SIZE;
    const batch = values.slice(start, end);

    if (batch.length === 0) break;

    console.log(`Starting to process ${batch.length} rows.`);

    const insertValues = batch.join(',');

    const insertQuery =
      `
      INSERT INTO clips (id, client_id, path, sentence, original_sentence_id, duration)
      VALUES ${insertValues}
      ON DUPLICATE KEY UPDATE
          duration = VALUES(duration);
      `;

    if (process.env.npm_config_dryRun) {
      console.log(insertQuery.slice(0, 100));
      start = end;
      continue;
    }

    db.beginTransaction((err) => {
      if (err) { throw err; }

      db.query(
        insertQuery,
        (err, results, fields) => {
          if (err) {
            return db.rollback(() => { throw err; });
          };

          db.commit((err) => {
            if (err) {
              return db.rollback(() => { throw err; });
            }

            current += batch.length;
            start = end;
            console.log(`Processed ${results.affectedRows} rows.`);
            console.log(`Progress: ${current}/${total}\n\n`);
          });
        }
      );
    });

    await new Promise(resolve => setTimeout(resolve, 5000));
  }
};

const run = async () => {
  const locales = loadStatsFromDisk(RELEASE_NAME).locales;

  for (const locale of locales) {
    const idsAndLocales = getIdsAndDurationFromLocale(locale);
    console.log(`Starting update for locale: '${locale}'`);
    await batchUpdateClipsTable(idsAndLocales);
  }
};

run();
