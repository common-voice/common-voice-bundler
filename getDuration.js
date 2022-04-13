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

const TSV_OPTIONS = {
  headers: true,
  delimiter: '\t',
  quote: false,
};

const getDuration = (directoryPath, clipPath, languageCode, tsvStream) => {
  return new Promise((resolve, reject) => {
    try {
      ffmpeg(path.join(directoryPath, clipPath)).ffprobe(
        0,
        function (err, data) {
          const dur = data.format.duration * 1000;
          // console.log('t', totalClipsDuration);
          tsvStream.write({
            duration: dur,
            languagecode: languageCode,
            path: clipPath,
          });
          resolve(dur);
        }
      );
    } catch (error) {
      reject(error);
    }
  });
};

const main = async (directoryPath, durationLedgerFilePath) => {
  try {
    let totalClipsDuration = 0;
    //get the language based on the path (<langCode>/clips)
    let languageCode = path.dirname(directoryPath);
    languageCode = path.basename(languageCode);

    const clipPaths = await fsPromise.readdir(directoryPath);
    const tsvStream = csv.format(TSV_OPTIONS);
    tsvStream.pipe(
      fs.createWriteStream(path.join(__dirname, durationLedgerFilePath))
    );

    console.time('Start of reading MP3');
    const promises = clipPaths.map((clipPath) => {
      return getDuration(directoryPath, clipPath, languageCode, tsvStream);
    });

    const durs = await Promise.all(promises);
    totalClipsDuration = durs.reduce((sum, dur) => {
      return sum + dur;
    }, 0);

    // ffmpeg(path.join(directoryPath, clipPath)).ffprobe(
    //   0,
    //   function (err, data) {
    //     const dur = data.format.duration * 1000;
    //     totalClipsDuration += dur;
    //     // console.log('t', totalClipsDuration);
    //     tsvStream.write({
    //       duration: dur,
    //       languagecode: languageCode,
    //       path: clipPath,
    //     });
    //     return totalClipsDuration;
    //   }
    // );
    // console.log('t', totalClipsDuration);
    // }
    console.timeEnd('Start of reading MP3');
    console.log(
      `Language ${languageCode} has a total duration of ${totalClipsDuration}ms`
    );
    tsvStream.end();
    return totalClipsDuration;
  } catch (error) {
    console.error(error);
  }
};

main('/home/g/Documents/cv-dataset/az/clips', 'clip-durations.tsv');
