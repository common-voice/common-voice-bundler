const fs = require('fs');
const path = require('path');
const readline = require('readline');
const csv = require('fast-csv');
const config = require('./config');

const { hashId, objectMap, mkDirByPathSync, append } = require('./helpers');
const { updateClipStats, formatFinalClipsStats } = require('./processStats');
const errors = { tooSmall: {}, notFound: {}};

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
  let activeWriteStreams = 0;
  let rowIndex = 0;
  let clipSavedIndex = 0;
  let readAllRows = false;
  let stats = {};

  const tsvStream = csv.createWriteStream(TSV_OPTIONS);
  tsvStream.pipe(fs.createWriteStream(path.join(releaseName, 'clips.tsv')));

  return new Promise(resolve => {
    const cleanUp = () => {
      if (readAllRows && activeBucketConnections == 0 && activeWriteStreams == 0) {
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

        resolve(formatFinalClipsStats(releaseName, stats));
      }
    };

    const updateDbStatus = () => {
      if (activeBucketConnections > 50 || activeWriteStreams >  50) {
        db.pause();
      }

      if (activeBucketConnections < 25 && activeWriteStreams < 25) {
        db.resume();
      }

      cleanUp();
    }

    const appendToTsv = (row, filePath) => {
      activeWriteStreams++;
      updateDbStatus();

      tsvStream.write({
        ...row,
        sentence: row.sentence.split('\r').join(' '),
        client_id: config.get('skipHashing')
          ? row.client_id
          : hashId(row.client_id),
        filePath,
      }, () => {
        activeWriteStreams--;
        updateDbStatus();
      });
    }

    const renderProgress = () => {
      process.stdout.write(
        `${rowIndex} rows processed, ${clipSavedIndex} downloaded\r`
      );
    };

    const downloadClipFile = (path) => {
      activeBucketConnections++;
      updateDbStatus();

      return clipBucket.bucket
        .getObject({
          Bucket: clipBucket.name,
          Key: path,
        });
    };

    const getMetadata = async (row) => {
      activeBucketConnections++;
      updateDbStatus();

      return clipBucket.bucket
        .headObject({ Key: row.path, Bucket: clipBucket.name })
        .promise()
        .then(res => res.ContentLength)
        .catch(err => {
          throw err;
        }).finally(() => {
          activeBucketConnections--;
          updateDbStatus();
        });
    };

    db.query(fs.readFileSync(QUERY_FILE, 'utf-8'))
      .on('result', row => {
        rowIndex++;
        renderProgress(rowIndex, clipSavedIndex);

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
          appendToTsv(row, newPath)
          return;
        }

        getMetadata(row).then(metadata => {
          if (metadata.ContentLength <= 256) {
            if (errors.tooSmall[row.locale] === undefined) errors.tooSmall[row.locale] = [];
            errors.tooSmall[row.locale].push({
              path: row.path,
              size: metadata.ContentLength,
            });
            return;
          } else {
            appendToTsv(row, newPath);

            if (config.get('skipDownload')) {
              cleanUp();
              return;
            }

            mkDirByPathSync(clipsDir);

            downloadClipFile(row.path)
              .createReadStream()
              .pipe(fs.createWriteStream(soundFilePath))
              .on('finish', () => {
                clipSavedIndex++;
                renderProgress(rowIndex, clipSavedIndex);

                activeBucketConnections--;
                updateDbStatus();
              });
          }

          cleanUp();
        }).catch((e) => {
          if (errors.notFound[row.locale] === undefined) errors.notFound[row.locale] = [];
          errors.notFound[row.locale].push({
            path: row.path
          });
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
