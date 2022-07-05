const fsPromise = require('fs').promises;
var ffmpeg = require('fluent-ffmpeg');
const path = require('path');

const _getDuration = (directoryPath, clipPath, languageCode) => {
  return new Promise((resolve, reject) => {
    ffmpeg(path.join(directoryPath, clipPath)).ffprobe(0, function (err, data) {
      if (err) {
        reject(err);
      }
      let duration =
        data &&
        data.format &&
        data.format.duration &&
        data.format.duration * 1000;
      if (!duration) reject();
      duration = Number.parseFloat(duration).toFixed(0);
      resolve([duration, languageCode, clipPath]);
    });
  });
};

const makeData = async (directoryPath, clipPath, languageCode, pathf) => {
  try {
    const data = await _getDuration(
      directoryPath,
      clipPath,
      languageCode,
      pathf
    );
    await fsPromise.writeFile(pathf, data.toString() + ',\n', {
      flag: 'a',
    });
    return data;
  } catch (error) {
    console.error(error);
  }
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
  const pa = path.join(__dirname, durationLedgerFilePath);
  const clipList = [];

  await Promise.all(
    clipPaths.map((clipPath) => {
      return makeData(directoryPath, clipPath, languageCode, pa);
    })
  );

  console.log(
    `Language ${languageCode} has a total duration of ${totalClipsDuration}ms`
  );

  return new Promise((resolve, reject) => {
    resolve({ languageCode, totalClipsDuration });
  });
};

module.exports = {
  getDuration,
};

const getFilesFromDirectory = async (directoryPath, callback) => {
  return await fsPromise.readdir(directoryPath);
};

(async () => {
  const args = process.argv.slice(2);

  try {
    const DIRECTORY_PATH =
      args[0] || '/home/ubuntu/cv-bundler/cv-corpus-9.0-2022-04-27/';
    const dirs = await getFilesFromDirectory(DIRECTORY_PATH);

    await Promise.all(
      dirs.map((langPath) => {
        const lang = path.basename(langPath);
        console.log('Reading ', lang, langPath);
        return getDuration(
          lang,
          DIRECTORY_PATH + lang + '/clips',
          lang + '-clip-durations.csv'
        );
      })
    );
  } catch (error) {
    console.error(error);
  }
})();
