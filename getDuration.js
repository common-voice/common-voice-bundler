const fs = require('fs');
const fsPromise = require('fs').promises;
var ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const csv = require('fast-csv');

// const config = require('./config');
// pass a directory
// loop through every mp3 file
// open and find mp3 duration
// save to main log (clip-durations.tsv)
// return total duration for directory

const TSV_OPTIONS = {
  headers: true,
  delimiter: '\t',
  quote: false,
};

const _getDuration = (directoryPath, clipPath, languageCode, tsvStream) => {
  return new Promise((resolve, reject) => {
    try {
      ffmpeg(path.join(directoryPath, clipPath)).ffprobe(
        0,
        function (err, data) {
          let duration = data.format.duration * 1000;
          duration = Number.parseFloat(duration).toFixed(0);
          tsvStream.write([duration, languageCode, clipPath]);
          resolve({ duration, languageCode, clipPath });
        }
      );
    } catch (error) {
      reject(error);
    }
  });
};

/**
 *
 * @param {string} languageCode Language Code (e.g. 'en')
 * @param {string} directoryPath Path to directory where clips are located
 * @param {string} durationLedgerFilePath Path to file where clip data is stored
 * @returns Promise<{languageCode, totalClipDuration}>
 */
const getDuration = async (
  languageCode,
  directoryPath,
  durationLedgerFilePath
) => {
  let totalClipsDuration = 0;
  const clipPaths = await fsPromise.readdir(directoryPath);
  const tsvStream = csv.format(TSV_OPTIONS);
  tsvStream.pipe(
    fs.createWriteStream(path.join(__dirname, durationLedgerFilePath))
  );
  // console.time('start');
  const promises = await clipPaths.map((clipPath) => {
    return _getDuration(directoryPath, clipPath, languageCode, tsvStream);
  });
  const clipList = await Promise.all(promises);
  totalClipsDuration = clipList.reduce((sum, clip) => {
    return sum + +clip.duration;
  }, 0);

  // console.timeEnd('start');
  console.log(
    `Language ${languageCode} has a total duration of ${totalClipsDuration}ms`
  );
  tsvStream.end();
  return new Promise((resolve, reject) => {
    resolve({ languageCode, totalClipsDuration });
  });
};

module.exports = {
  getDuration,
};

(async () => {
  getDuration(
    'az',
    '/home/g/Documents/cv-dataset/az/clips',
    'clip-durations.tsv'
  );
})();
