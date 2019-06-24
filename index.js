const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { PassThrough } = require('stream');
const S3 = require('aws-sdk/clients/s3');
const csv = require('fast-csv');
const merge = require('lodash.merge');
const mysql = require('mysql');
const { spawn } = require('promisify-child-process');
const tar = require('tar');
const config = require('./config');
const {
  countFileLines,
  logProgress,
  mkDirByPathSync,
  objectMap
} = require('./helpers');

const TSV_OPTIONS = {
  headers: true,
  delimiter: '\t',
  quote: false
};
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
const releaseDir = config.get('releaseName');

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

function formatStats(localeSplits) {
  return objectMap(localeSplits, ({ clips, splits, usersSet }) => ({
    clips,
    splits: objectMap(splits, values =>
      objectMap(values, value => Number((value / clips).toFixed(2)))
    ),
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
    if (!config.get('skipHashing')) {
      tsvStream.pipe(fs.createWriteStream(TSV_PATH));
    }

    let readAllRows = false;
    const cleanUp = () => {
      if (readAllRows && activeDownloads == 0) {
        db.end();
        console.log('');
        resolve(formatStats(stats));
      }
    };

    const stats = {};
    db.query(fs.readFileSync(path.join(__dirname, 'query.sql'), 'utf-8'))
      .on('result', row => {
        rowIndex++;
        renderProgress();

        const localeStats =
          stats[row.locale] ||
          (stats[row.locale] = {
            clips: 0,
            splits: { accent: {}, age: {}, gender: {} },
            usersSet: new Set()
          });
        const { splits, usersSet } = localeStats;
        localeStats.clips++;

        for (const key of Object.keys(splits).filter(key => key != 'filter')) {
          const value = row[key];
          splits[key][value] = (splits[key][value] || 0) + 1;
        }

        usersSet.add(row.client_id);

        if (config.get('skipHashing')) return;

        const newPath = `common_voice_${row.locale}_${row.id}.mp3`;
        tsvStream.write({
          ...row,
          sentence: row.sentence.split('\r').join(' '),
          client_id: crypto
            .createHash('sha256')
            .update(row.client_id)
            .digest('hex'),
          path: newPath
        });

        const clipsDir = path.join(OUT_DIR, row.locale, 'clips');
        const soundFilePath = path.join(clipsDir, newPath);

        if (fs.existsSync(soundFilePath)) {
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
  // const child = await spawn('create-corpora', ['-f', TSV_PATH, '-d', OUT_DIR], {
  //   encoding: 'utf8'
  // });
  // if (child.error) {
  //   throw child.error;
  // }

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

const sumDurations = async () => {
  const durations = {};
  for (const locale of getLocaleDirs()) {
    durations[locale] = {
      duration: Number(
        (await spawn(
          'mp3-duration-sum',
          [path.join(OUT_DIR, locale, 'clips')],
          {
            encoding: 'utf8'
          }
        )).stdout
      )
    };
  }
  return durations;
};

const archiveAndUpload = () =>
  getLocaleDirs().reduce((promise, locale) => {
    return promise.then(sizes => {
      const stream = new PassThrough();
      const archiveName = `${releaseDir}/${locale}.tar.gz`;
      console.log('archiving & uploading', archiveName);
      const managedUpload = outBucket.upload({
        Body: stream,
        Bucket: outBucketName,
        Key: archiveName,
        ACL: 'public-read'
      });
      logProgress(managedUpload);

      const localeDir = path.join(OUT_DIR, locale);
      tar
        .c({ gzip: true, cwd: localeDir }, fs.readdirSync(localeDir))
        .pipe(stream);

      return managedUpload
        .promise()
        .then(() =>
          outBucket
            .headObject({ Bucket: outBucketName, Key: archiveName })
            .promise()
        )
        .then(({ ContentLength }) => {
          console.log('');
          sizes[locale] = { size: ContentLength };
          return sizes;
        })
        .catch(err => console.error(err));
    });
  }, Promise.resolve({}));

const collectAndUplodatStats = async stats => {
  const statsJSON = {
    bundleURLTemplate: `https://${outBucketName}.s3.amazonaws.com/${releaseDir}/{locale}.tar.gz`,
    locales: merge(...stats)
  };
  console.dir(statsJSON, { depth: null, colors: true });
  return outBucket
    .putObject({
      Body: JSON.stringify(statsJSON),
      Bucket: outBucketName,
      Key: `${releaseDir}/stats.json`,
      ACL: 'public-read'
    })
    .promise();
};

processAndDownloadClips()
  .then(stats =>
    Promise.all([
      stats,
      sumDurations(),
      countBuckets().then(async bucketStats =>
        merge(
          bucketStats,
          await (config.get('skipBundling')
            ? Promise.resolve()
            : archiveAndUpload())
        )
      )
    ])
  )
  .then(collectAndUplodatStats)
  .catch(e => console.error(e));
