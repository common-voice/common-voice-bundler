const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const { saveStatsToDisk } = require('./processStats');

const getReportedSentences = (db, localeDirs, releaseName) => {
  const QUERY_FILE = path.join(
    __dirname,
    'queries',
    'getReportedSentences.sql'
  );

  const TSV_OPTIONS = {
    headers: true,
    delimiter: '\t',
    quote: false,
  };

  return new Promise(resolve => {
    const reportedSentences = {};

    db.query(fs.readFileSync(QUERY_FILE, 'utf-8'))
      .on('result', row => {
        if (!localeDirs.includes(row.locale)) return;

        if (reportedSentences[row.locale] === undefined) {
          reportedSentences[row.locale] = [];
        }

        reportedSentences[row.locale].push({
          ...row,
          sentence: row.sentence.split('\r').join(' '),
        });
      })
      .on('end', () => {
        Object.keys(reportedSentences).map(locale => {
          const localePath = path.join(
            __dirname,
            releaseName,
            locale,
            'reported.tsv'
          );

          csv
            .write(reportedSentences[locale], TSV_OPTIONS)
            .pipe(fs.createWriteStream(localePath));

          reportedSentences[locale] = {
            reportedSentences: reportedSentences[locale].length,
          };
        });

        saveStatsToDisk(releaseName, { locales: reportedSentences });
        resolve(reportedSentences);
      });
  });
};

module.exports = {
  getReportedSentences,
};
