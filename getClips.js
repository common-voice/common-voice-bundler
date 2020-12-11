const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const config = require('./config');

const { hashId, mkDirByPathSync } = require('./helpers');
const { updateClipStats, formatFinalClipsStats } = require('./processStats');

const errors = { tooSmall: {}, notFound: {} };
const TSV_OPTIONS = {
  headers: true,
  delimiter: '\t',
  quote: false,
};

/**
 * Main function for processing and downloading clips
 *
 * @param {Object} db              db connection
 * @param {Object} clipBucket      datasets bucket object with name and bucket keys
 * @param {string} releaseName     name of current release
 * @param {array} minorityLangs    array of languages with fewer than 5 speakers
 *
 * @return {Object} locale-indexed stats object
 */
const processAndDownloadClips = (
  db,
  clipBucket,
  releaseName,
  minorityLangs,
) => {
  const queryFile = path.join(__dirname, 'queries', config.get('queryFile'));

  // Counters for performance optimization and logging
  let activeBucketConnections = 0;
  let activeWriteStreams = 0;
  let rowIndex = 0;
  let clipSavedIndex = 0;

  // current states
  let readAllRows = false;
  let stats = {};

  // read and write streams for TSV data
  const tsvStream = csv.createWriteStream(TSV_OPTIONS);
  tsvStream.pipe(fs.createWriteStream(path.join(releaseName, 'clips.tsv')));

  return new Promise((resolve) => {
    // cleanUp function to be run at the end of every row to see if everything
    // has been completed
    const cleanUp = () => {
      if (
        readAllRows
        && activeBucketConnections === 0
        && activeWriteStreams === 0
      ) {
        console.log('');
        tsvStream.end();

        // write errors to disk
        fs.writeFileSync(
          path.join(__dirname, releaseName, 'errors.json'),
          JSON.stringify(errors),
          'utf8',
          (err) => {
            if (err) throw err;
          },
        );

        // format stats and return to main run function
        resolve(formatFinalClipsStats(releaseName, stats));
      }
    };

    // Helper function to pause and restart stream depending on
    // how many active connections there are, to prevent running out of memory
    const updateDbStatus = () => {
      if (activeBucketConnections > 50 || activeWriteStreams > 50) {
        db.pause();
      }

      if (activeBucketConnections < 25 && activeWriteStreams < 25) {
        db.resume();
      }

      cleanUp();
    };

    // Helper function to write current row to master TSV file
    const appendToTsv = (row, filePath) => {
      activeWriteStreams++;
      updateDbStatus();

      tsvStream.write(
        {
          ...row,
          sentence: row.sentence.replace(/\s/gi, ' '),
          client_id: config.get('skipHashing')
            ? row.client_id
            : hashId(row.client_id),
          path: filePath,
        },
        () => {
          activeWriteStreams--;
          updateDbStatus();
        },
      );
    };

    // Helper function to render current progress
    const renderProgress = () => {
      process.stdout.write(
        `${rowIndex} rows processed, ${clipSavedIndex} downloaded\r`,
      );
    };

    // Helper function to download a file
    const downloadClipFile = (clipPath) => {
      activeBucketConnections++;
      updateDbStatus();

      return clipBucket.bucket.getObject({
        Bucket: clipBucket.name,
        Key: clipPath,
      });
    };

    // Helper function to get filesize metadata for function
    const getMetadata = async (row) => {
      activeBucketConnections++;
      updateDbStatus();

      return clipBucket.bucket
        .headObject({ Key: row.path, Bucket: clipBucket.name })
        .promise()
        .then((res) => res.ContentLength)
        .catch((err) => {
          throw err;
        })
        .finally(() => {
          activeBucketConnections--;
          updateDbStatus();
        });
    };

    // Main query for bundling
    db.query(fs.readFileSync(queryFile, 'utf-8'), [config.get('cutoffTime')])
      .on('result', (dbRow) => {
        const row = dbRow;
        rowIndex++;
        renderProgress(rowIndex, clipSavedIndex);

        // Scrub demographic info if it's a minority language
        if (minorityLangs.includes(row.locale)) {
          row.gender = '';
          row.age = '';
        }

        const clipsDir = path.join(releaseName, row.locale, 'clips');
        const newPath = `common_voice_${row.locale}_${row.id}.mp3`;
        const soundFilePath = path.join(clipsDir, newPath);

        // If audio file has previously been downloaded, update stats/TSV immediately
        if (
          fs.existsSync(soundFilePath)
          && fs.statSync(soundFilePath).size > 0
        ) {
          stats = updateClipStats(stats, row);
          appendToTsv(row, newPath);
          return;
        }

        // Get filesize of clip and skip if it's smaller than 256 (blank clips)
        getMetadata(row)
          .then((metadata) => {
            if (metadata <= 256) {
              if (errors.tooSmall[row.locale] === undefined) {
                errors.tooSmall[row.locale] = [];
              }

              // If file is too small, append to error object
              errors.tooSmall[row.locale].push({
                path: row.path,
                size: metadata.ContentLength,
              });
            } else {
              // If valid clip, update clipStats and add to TSV
              stats = updateClipStats(stats, row);
              appendToTsv(row, newPath);

              if (config.get('skipDownload')) {
                return;
              }

              // Prepare clips path
              mkDirByPathSync(clipsDir);

              // Download clip
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
          })
          .catch(() => {
            // If file does not exist, append to error object
            if (errors.notFound[row.locale] === undefined) {
              errors.notFound[row.locale] = [];
            }
            errors.notFound[row.locale].push({
              path: row.path,
            });
          })
          .finally(() => {
            // Once all promises resolve, perform cleanup and check status
            cleanUp();
          });
      })
      .on('end', () => {
        // Once db query completes, set status to read and perform cleanup
        readAllRows = true;
        cleanUp();
      });
  });
};

module.exports = {
  processAndDownloadClips,
};
