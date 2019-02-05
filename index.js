const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { PassThrough } = require('stream');
const S3 = require('aws-sdk/clients/s3');
const csv = require('fast-csv');
const merge = require('lodash.merge');
const mp3Duration = require('mp3-duration');
const mysql = require('mysql');
const { spawn } = require('promisify-child-process');
const tar = require('tar');
const config = require('./config');
const {
  countFileLines,
  hash,
  logProgress,
  mkDirByPathSync,
  objectMap
} = require('./helpers');

const TSV_OPTIONS = { headers: true, delimiter: '\t', quote: null };
const OUT_DIR = 'out';
const TSV_PATH = path.join(OUT_DIR, 'clips.tsv');

const { accessKeyId, secretAccessKey, name: outBucketName } = config.get(
  'outBucket'
);

const outBucket = new S3({
  ...(accessKeyId
    ? {
        credentials: {
          accessKeyId,
          secretAccessKey
        }
      }
    : {}),
  region: 'us-west-2'
});
const releaseDir = 'cv-corpus-' + new Date().toISOString().split('T')[0];

const downloadClipFile = path => {
  const { accessKeyId, secretAccessKey, name, region } = config.get(
    'clipBucket'
  );
  return new S3({
    ...(accessKeyId
      ? {
          credentials: {
            accessKeyId,
            secretAccessKey
          }
        }
      : {}),
    region
  }).getObject({
    Bucket: name,
    Key: path
  });
};

function formatDemographics(localeSplits) {
  return objectMap(localeSplits, ({ splits, usersSet }) => ({
    splits: Object.entries(splits)
      .filter(([key]) => key != 'total')
      .reduce((result, [key, values]) => {
        result[key] = objectMap(values, value =>
          Number((value / splits.total).toFixed(2))
        );
        return result;
      }, {}),
    users: usersSet.size
  }));
}

const processAndDownloadClips = () => {
  const { host, user, password, database } = config.get('db');
  const db = mysql.createConnection({
    host,
    user,
    password,
    database
  });
  db.connect();

  return new Promise(resolve => {
    let activeDownloads = 0;
    let rowIndex = 0;
    let clipSavedIndex = 0;
    const renderProgress = () => {
      readline.cursorTo(process.stdout, 0);
      process.stdout.write(
        rowIndex + ' rows processed, ' + clipSavedIndex + ' clips downloaded'
      );
    };

    const tsvStream = csv.createWriteStream(TSV_OPTIONS);
    tsvStream.pipe(fs.createWriteStream(TSV_PATH));

    let readAllRows = false;
    const cleanUp = () => {
      if (readAllRows && activeDownloads == 0) {
        db.end();
        console.log('');
        resolve(formatDemographics(localeSplits));
      }
    };

    const localeSplits = {};
    db.query(fs.readFileSync(path.join(__dirname, 'query.sql'), 'utf-8'))
      .on('result', row => {
        rowIndex++;
        renderProgress();

        const { splits, usersSet } =
          localeSplits[row.locale] ||
          (localeSplits[row.locale] = {
            splits: { total: 0, accent: {}, age: {}, gender: {} },
            usersSet: new Set()
          });
        splits.total++;

        for (const key of Object.keys(splits).filter(key => key != 'filter')) {
          const value = row[key];
          splits[key][value] = (splits[key][value] || 0) + 1;
        }

        usersSet.add(row.client_id);

        const newPath = hash(row.path);
        tsvStream.write({
          ...row,
          client_id: hash(row.client_id),
          path: newPath
        });

        const clipsDir = path.join(OUT_DIR, row.locale, 'clips');
        const soundFilePath = path.join(clipsDir, newPath + '.mp3');

        if (config.get('skipDownload') || fs.existsSync(soundFilePath)) {
          return;
        }

        if (activeDownloads > 50) {
          db.pause();
        }

        activeDownloads++;

        mkDirByPathSync(clipsDir);
        downloadClipFile(row.path)
          .createReadStream()
          .pipe(fs.createWriteStream(soundFilePath))
          .on('finish', () => {
            activeDownloads--;
            if (activeDownloads < 25) {
              db.resume();
            }

            clipSavedIndex++;
            renderProgress();

            cleanUp();
          });
      })
      .on('end', () => {
        readAllRows = true;
        tsvStream.end();
        cleanUp();
      });
  });
};

function getLocaleDirs() {
  return fs
    .readdirSync(OUT_DIR)
    .filter(f => fs.statSync(path.join(OUT_DIR, f)).isDirectory());
}

const countBuckets = async () => {
  const child = await spawn('create-corpora', ['-f', TSV_PATH, '-d', OUT_DIR], {
    encoding: 'utf8'
  });
  if (child.error) {
    throw child.error;
  }

  const buckets = {};
  for (const locale of getLocaleDirs()) {
    const localePath = path.join(OUT_DIR, locale);
    const localeBuckets = (await fs.readdirSync(localePath))
      .filter(file => file.endsWith('.tsv'))
      .map(async fileName => [
        fileName,
        Math.max((await countFileLines(path.join(localePath, fileName))) - 1, 0)
      ]);
    buckets[locale] = {
      buckets: (await Promise.all(localeBuckets)).reduce(
        (obj, [key, count]) => {
          obj[key.split('.tsv')[0]] = count;
          return obj;
        },
        {}
      )
    };
  }
  return buckets;
};

const countClipsAndDuration = async () => {
  const durations = {};
  for (const locale of getLocaleDirs()) {
    const clipsPath = path.join(OUT_DIR, locale, 'clips');
    const files = await fs.readdirSync(clipsPath);
    const duration = await files.reduce(
      (promise, file) =>
        promise.then(
          async sum => sum + (await mp3Duration(path.join(clipsPath, file)))
        ),
      Promise.resolve(0)
    );
    durations[locale] = {
      clips: files.length,
      duration: Math.floor(duration)
    };
  }
  return durations;
};

const archiveAndUpload = () =>
  getLocaleDirs().reduce((promise, locale) => {
    return promise.then(() => {
      console.log('archiving & uploading', locale);

      const stream = new PassThrough();
      const managedUpload = outBucket.upload({
        Body: stream,
        Bucket: outBucketName,
        Key: `${releaseDir}/${locale}.tar.gz`
      });
      logProgress(managedUpload);

      const localeDir = path.join(OUT_DIR, locale);
      tar
        .c({ gzip: true, cwd: localeDir }, fs.readdirSync(localeDir))
        .pipe(stream);

      return managedUpload
        .promise()
        .then(() => console.log(''))
        .catch(err => console.error(err));
    });
  }, Promise.resolve());

const collectAndUplodatStats = async demographics => {
  const stats = merge(
    ...(await Promise.all([
      demographics,
      countBuckets(),
      countClipsAndDuration()
    ]))
  );
  console.dir(stats, { depth: null, colors: true });
  return outBucket
    .putObject({
      Body: JSON.stringify(stats),
      Bucket: outBucketName,
      Key: `${releaseDir}/stats.json`
    })
    .promise();
};

processAndDownloadClips()
  .then(
    demographics =>
      !config.get('skipBundling') &&
      Promise.all([collectAndUplodatStats(demographics), archiveAndUpload()])
  )
  .catch(e => console.error(e));
