const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const { saveStatsToDisk } = require('./processStats');

/**
 * Download all reported sentences for each language
 *
 * @param {Object} db          database connection
 * @param {array} localeDirs   array of locale directories
 * @param {string} releaseName name of current release
 *
 * @return {Object} key/value pairs of locale { reportedSentences: # }
 *                  to merge w Stats obj
 */
const getReportedSentences = (db, localeDirs, releaseName) => {
  const QUERY_FILE = path.join(
    __dirname,
    'queries',
    'getReportedSentences.sql',
  );

  const TSV_OPTIONS = {
    headers: true,
    delimiter: '\t',
    quote: false,
  };

  return new Promise((resolve) => {
    const reportedSentences = {};

    db.query(fs.readFileSync(QUERY_FILE, 'utf-8'))
      .on('result', (row) => {
        // if locale is not included in current set of releases, skip
        if (!localeDirs.includes(row.locale)) return;

        // initiate locale in results object
        if (reportedSentences[row.locale] === undefined) {
          reportedSentences[row.locale] = [];
        }

        // add reported sentence
        reportedSentences[row.locale].push({
          ...row,
          sentence: row.sentence.split('\r').join(' '),
        });
      })
      .on('end', () => {
        // When complete, write all reported sentences to TSV
        Object.keys(reportedSentences).forEach((locale) => {
          const localePath = path.join(
            __dirname,
            releaseName,
            locale,
            'reported.tsv',
          );

          csv
            .write(reportedSentences[locale], TSV_OPTIONS)
            .pipe(fs.createWriteStream(localePath));

          reportedSentences[locale] = {
            reportedSentences: reportedSentences[locale].length,
          };
        });

        // Merge with existing stats and return reported sentence counts
        saveStatsToDisk(releaseName, { locales: reportedSentences });
        resolve(reportedSentences);
      });
  });
};

module.exports = {
  getReportedSentences,
};
