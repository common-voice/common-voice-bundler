require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { PassThrough } = require('stream');
const tar = require('tar');
const config = require('./config');

const { logProgress, sumDurations, getLocaleDirs } = require('./helpers');

const outBucketName = config.get('outBucket');

const { db, clipBucket, bundlerBucket } = require('./init').initialize();
const { processAndDownloadClips } = require('./getClips');

const releaseDir = config.get('releaseName');

const zipAndUpload = async () => {
  const stream = new PassThrough();
  const name = 'eng_anon_25h_valid';

  const archiveName = `${releaseDir}/${name}.tar.gz`;
  console.log('archiving & uploading', archiveName);
  const managedUpload = bundlerBucket.bucket.upload({
    Body: stream,
    Bucket: outBucketName,
    Key: archiveName,
    ACL: 'public-read',
  });
  logProgress(managedUpload);

  const cwd = path.join(releaseDir);
  tar.c({ gzip: true, cwd }, fs.readdirSync(cwd)).pipe(stream);

  return managedUpload
    .promise()
    .then(() => bundlerBucket.bucket
      .headObject({ Bucket: bundlerBucket.name, Key: archiveName })
      .promise())
    .then(({ ContentLength }) => {
      console.log('');
      return ContentLength;
    })
    .catch((err) => console.error(err));
};

processAndDownloadClips(db, clipBucket)
  .then((stats) => Promise.all([stats, sumDurations(getLocaleDirs()), zipAndUpload()]))
  .catch((e) => console.error(e));
