const fs = require('fs');
const path = require('path');
const readline = require('readline');
const crypto = require('crypto');

const prompt = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

function bytesToSize(bytes) {
  var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  if (bytes == 0) return '0 Byte';
  var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
  return Math.round(bytes / Math.pow(1024, i), 2) + ' ' + sizes[i];
}

function countFileLines(filePath) {
  return new Promise((resolve, reject) => {
    let lineCount = 0;
    fs.createReadStream(filePath)
      .on('data', buffer => {
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

function logProgress(managedUpload, fileName) {
  managedUpload.on('httpUploadProgress', progress => {
    process.stdout.write(`${fileName} ${bytesToSize(progress.loaded)} upload progress\r`);
  });
}

function mkDirByPathSync(targetDir) {
  const sep = path.sep;
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

function promptAsync(question) {
  return new Promise(resolve => {
    prompt.question(question, resolve);
  });
}

async function promptLoop(prompt, options) {
  const answer = await promptAsync(prompt);
  const callback = options[answer.toLowerCase()];

  if (callback) await callback();
  else await promptLoop(promptLoop, options);
}

function objectMap(object, mapFn) {
  return Object.keys(object).reduce((result, key) => {
    result[key] = mapFn(object[key]);
    return result;
  }, {});
}

function unitToHours(duration, unit, sigDig) {
  let perHr = 1;
  const sigDigMultiplier = Math.pow(10, sigDig);

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
  }

  return Math.floor((duration / perHr) * sigDigMultiplier) / sigDigMultiplier;
}

function hashId(id) {
  return crypto.createHash('sha512').update(id).digest('hex');
}

module.exports = {
  countFileLines,
  logProgress,
  mkDirByPathSync,
  promptLoop,
  unitToHours,
  objectMap,
  hashId,
  bytesToSize,
};
