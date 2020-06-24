const fs = require('fs');
const path = require('path');
const readline = require('readline');
const csv = require('fast-csv');
const config = require('./config');

const { hashId, objectMap, mkDirByPathSync, append } = require('./helpers');

const { updateClipStats, formatFinalClipsStats } = require('./processStats');

const renderProgress = (rows, clips, downloads) => {
  process.stdout.write(
    `${rows} rows processed, ${clips} checked, ${downloads} downloaded`
  );
};

const downloadClipFile = (clipBucket, path) => {
  return clipBucket.bucket.getObject({
    Bucket: clipBucket.name,
    Key: path,
  });
};

const getMetadata = async (clipBucket, path) => {
  return clipBucket.bucket
    .headObject({ Key: path, Bucket: clipBucket.name })
    .promise()
    .then(res => res)
    .catch(err => console.log(err));
};

const processAndDownloadClips = (
  db,
  clipBucket,
  releaseName,
  minorityLangs
) => {
  const QUERY_FILE = path.join(__dirname, 'queries', config.get('queryFile'));
  const TSV_OPTIONS = {
    headers: true,
    delimiter: '\t',
    quote: false,
  };

  let activeBucketConnections = 0;
  let rowIndex = 0;
  let clipSavedIndex = 0;
  let clipCheckedIndex = 0;
  let readAllRows = false;
  const stats = {};
  const errors = {};

  const tsvStream = csv.createWriteStream(TSV_OPTIONS);
  tsvStream.pipe(fs.createWriteStream(path.join(releaseName, 'clips.tsv')));

  return new Promise(resolve => {
    const cleanUp = () => {
      if (readAllRows && activeBucketConnections == 0) {
        console.log('');
        tsvStream.end();

        fs.appendFile(
          path.join(__dirname, releaseName, 'errors.json'),
          JSON.stringify(errors),
          'utf8',
          function (err) {
            if (err) throw err;
          }
        );

        resolve(formatFinalClipsStats(stats));
      }
    };

    db.query(fs.readFileSync(QUERY_FILE, 'utf-8'))
      .on('result', row => {
        rowIndex++;
        renderProgress(rowIndex, clipCheckedIndex, clipSavedIndex);

        if (minorityLangs.includes(row.locale)) {
          row.gender = '';
          row.age = '';
        }

        stats = updateClipStats(stats, row);

        const clipsDir = path.join(releaseName, row.locale, 'clips');
        const newPath = `common_voice_${row.locale}_${row.id}.mp3`;
        const soundFilePath = path.join(clipsDir, newPath);

        if (
          fs.existsSync(soundFilePath) &&
          fs.statSync(soundFilePath)['size'] > 0
        ) {
          return;
        }

        activeBucketConnections++;

        if (activeBucketConnections > 50) {
          db.pause();
        }

        getMetadata(clipBucket, row.path).then(metadata => {
          clipCheckedIndex++;
          activeBucketConnections--;

          if (activeBucketConnections < 25) {
            db.resume();
          }

          if (metadata.ContentLength <= 256) {
            if (errors[row.locale] === undefined) errors[row.locale] = [];
            errors[row.locale].push({
              path: row.path,
              size: metadata.ContentLength,
            });

            cleanUp();
            return;
          } else {
            tsvStream.write({
              ...row,
              sentence: row.sentence.split('\r').join(' '),
              client_id: config.get('skipHashing')
                ? row.client_id
                : hashId(row.client_id),
              path: newPath,
            });

            if (config.get('skipDownload')) {
              cleanUp();
              return;
            }

            activeBucketConnections++;

            mkDirByPathSync(clipsDir);

            downloadClipFile(clipBucket, row.path)
              .createReadStream()
              .pipe(fs.createWriteStream(soundFilePath))
              .on('finish', () => {
                activeBucketConnections--;
                if (activeBucketConnections < 25) {
                  db.resume();
                }

                clipSavedIndex++;
                renderProgress(rowIndex, clipCheckedIndex, clipSavedIndex);
                cleanUp();
              });
          }

          cleanUp();
        });
      })
      .on('end', () => {
        readAllRows = true;
        cleanUp();
      });
  });
};

module.exports = {
  processAndDownloadClips,
};
