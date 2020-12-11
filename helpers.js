const fs = require('fs');
const path = require('path');
const readline = require('readline');
const crypto = require('crypto');

const prompt = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

/**
 * Recursive helper function to sequentially create and resolve a series of Promises
 * over an array, and return all results
 *
 * @param {array} array         array of input values for promise
 * @param {array} resultStore   pointer to array that will store resolved results
 * @param {function} promiseFn  promise function
 */
const sequencePromises = (array, resultStore, promiseFn) => {
  promiseFn(array.shift()).then((result) => {
    resultStore.push(result);
    return array.length === 0
      ? resultStore
      : sequencePromises(array, resultStore, promiseFn);
  });
};

/**
 * Convert total byte count to human readable size
 *
 * @param {number} bytes   total number of bytes
 *
 * @return {string} human readable file size to 2 decimal points
 */
function bytesToSize(bytes) {
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  if (bytes === 0) return '0 Byte';
  const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)), 10);
  return `${Math.round(bytes / (1024 ** i), 2)} ${sizes[i]}`;
}

/**
 * Count number of lines in given file
 *
 * @param {string} filePath    full path of file
 *
 * @return {number} number of lines in file
 */
function countFileLines(filePath) {
  return new Promise((resolve, reject) => {
    let lineCount = 0;
    fs.createReadStream(filePath)
      .on('data', (buffer) => {
        let idx = -1;
        lineCount--; // Because the loop will run once for idx=-1
        do {
          idx = buffer.indexOf(10, idx + 1);
          lineCount++;
        } while (idx !== -1);
      })
      .on('end', () => {
        resolve(lineCount);
      })
      .on('error', reject);
  });
}

/**
 * Create target directory, recurring over subdirs if necessary
 *
 * @param {string} targetDir    full path of target directory to create
 *
 * @return {string} absolute value of final targetDir
 */
function mkDirByPathSync(targetDir) {
  const { sep } = path;
  const initDir = path.isAbsolute(targetDir) ? sep : '';

  return targetDir.split(sep).reduce((parentDir, childDir) => {
    const curDir = path.resolve('.', parentDir, childDir);
    try {
      fs.mkdirSync(curDir);
    } catch (err) {
      if (err.code === 'EEXIST') {
        // curDir already exists!
        return curDir;
      }

      // To avoid `EISDIR` error on Mac and `EACCES`-->`ENOENT` and `EPERM` on Windows.
      if (err.code === 'ENOENT') {
        // Throw the original parentDir error on curDir `ENOENT` failure.
        throw new Error(`EACCES: permission denied, mkdir '${parentDir}'`);
      }

      const caughtErr = ['EACCES', 'EPERM', 'EISDIR'].indexOf(err.code) > -1;
      if (!caughtErr || (caughtErr && curDir === path.resolve(targetDir))) {
        throw err; // Throw if it's just the last created dir.
      }
    }

    return curDir;
  }, initDir);
}

/**
 * Turn prompt into a promise
 *
 * @param {string} question     question to show user
 *
 * @return {Promise} promisify'd prompt object
 */
function promptAsync(question) {
  return new Promise((resolve) => {
    prompt.question(question, resolve);
  });
}

/**
 * Continue prompting for user input until valid option is provided
 *
 * @param {string} promptInstance       question to show user
 * @param {Object} options      key: correct answer; value: callback function
 *
 * @return {function} call the callback function
 */
async function promptLoop(promptInstance, options) {
  const answer = await promptAsync(prompt);
  const callback = options[answer.toLowerCase()];

  if (callback) await callback();
  else await promptLoop(promptInstance, options);
}

/**
 * Given an object, apply a consistent function to each value
 *
 * @param {Object} object       key/value pair object
 * @param {function} mapFn      the function to apply to each value
 *
 * @return {Object} transformed object
 */
function objectMap(object, mapFn) {
  return Object.keys(object).reduce((result, key) => {
    const newResult = result;
    newResult[key] = mapFn(object[key]);
    return newResult;
  }, {});
}

/**
 * Convert a given duration to # of hours
 *
 * @param {number} duration     integer of time duration
 * @param {string} unit         unit of duration - ms, s, min
 * @param {number} sigDig       # of decimals to include
 *
 * @return {number} number of hours to given significant digits
 */
function unitToHours(duration, unit, sigDig) {
  let perHr = 1;
  const sigDigMultiplier = 10 ** sigDig;

  switch (unit) {
    case 'ms':
      perHr = 60 * 60 * 1000;
      break;
    case 's':
      perHr = 60 * 60;
      break;
    case 'min':
      perHr = 60;
      break;
    default:
      perHr = 1;
      break;
  }

  return Math.floor((duration / perHr) * sigDigMultiplier) / sigDigMultiplier;
}

/**
 * Hash client ID
 *
 * @param {string} id     uuid of client
 *
 * @return {string}       sha512 hash of client id
 */
function hashId(id) {
  return crypto.createHash('sha512').update(id).digest('hex');
}

module.exports = {
  countFileLines,
  mkDirByPathSync,
  promptLoop,
  unitToHours,
  objectMap,
  hashId,
  bytesToSize,
  sequencePromises,
};
