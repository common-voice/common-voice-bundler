const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

const QUERY_FILE = path.join(__dirname, 'queries', 'getReportedSentences.sql');

const TSV_OPTIONS = {
  headers: true,
  delimiter: '\t',
  quote: false
};

const getReportedSentences = (db, localeDirs, releaseName) => {
  return new Promise(resolve => {
    const tsvStream = csv.createWriteStream(TSV_OPTIONS);
    const reportedSentences = {};

    db.query(fs.readFileSync(QUERY_FILE, 'utf-8'))
      .on('result', row => {
        if (!localeDirs.includes(row.locale)) return;

        if (typeof reportedSentences[row.locale] === undefined) {
          reportedSentences[row.locale] = { reportedSentences: 0 };
        }

        reportedSentences[row.locale].reportedSentences++;
        const tsvPath = path.join(__dirname, releaseName, row.locale);
        tsvStream.pipe(fs.createWriteStream(tsvPath));

        tsvStream.write({
          ...row,
          sentence: row.sentence.split('\r').join(' ')
        });
      })
      .on('end', () => {
        resolve(reportedSentences);
      })
  });
}

module.exports = {
  getReportedSentences
}