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
          tsvStream.write({
            duration: duration,
            languagecode: languageCode,
            path: clipPath,
          });
          resolve(duration);
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
 * @returns number
 */
const getDuration = async (
  languageCode,
  directoryPath,
  durationLedgerFilePath
) => {
  try {
    let totalClipsDuration = 0;
    //get the language based on the path (<langCode>/clips)
    // let languageCode = path.dirname(directoryPath);
    // languageCode = path.basename(languageCode);

    const clipPaths = await fsPromise.readdir(directoryPath);
    const tsvStream = csv.format(TSV_OPTIONS);
    tsvStream.pipe(
      fs.createWriteStream(path.join(__dirname, durationLedgerFilePath))
    );

    const promises = clipPaths.map((clipPath) => {
      return _getDuration(directoryPath, clipPath, languageCode, tsvStream);
    });

    const durs = await Promise.all(promises);
    totalClipsDuration = durs.reduce((sum, dur) => {
      return sum + dur;
    }, 0);

    console.log(
      `Language ${languageCode} has a total duration of ${totalClipsDuration}ms`
    );
    tsvStream.end();
    return totalClipsDuration;
  } catch (error) {
    console.error(error);
  }
};

module.exports = {
  getDuration,
};

// main('/home/g/Documents/cv-dataset/az/clips', 'clip-durations.tsv');
