const S3 = require('aws-sdk/clients/s3');
const fs = require('fs');
const { PassThrough } = require('stream');
const config = require('./config');
const { logProgress, sequencePromises } = require('./helpers');

const { accessKeyId, secretAccessKey, name: outBucketName } = config.get(
  'outBucket',
);

const outBucket = new S3({
  ...(accessKeyId
    ? {
      credentials: {
        accessKeyId,
        secretAccessKey,
      },
    }
    : {}),
  region: 'us-west-2',
});

const releaseName = config.get('releaseName');

const uploadLocale = (locale) => new Promise((resolve) => {
  const stream = new PassThrough();
  const archiveName = `${releaseName}/${locale}.tar.gz`;
  console.log('uploading', archiveName);
  const managedUpload = outBucket.upload({
    Body: stream,
    Bucket: outBucketName,
    Key: archiveName,
    ACL: 'public-read',
  });

  logProgress(managedUpload);

  fs.createReadStream(`${releaseName}/tarballs/${locale}.tar.gz`).pipe(stream);

  return managedUpload
    .promise()
    .then(() => outBucket
      .headObject({ Bucket: outBucketName, Key: archiveName })
      .promise()
      .then(() => resolve()))
    .catch((err) => console.error(err));
});

try {
  sequencePromises(stats.locales, [], uploadLocale)
    .then((stats) => {
      const mergedStats = merge(...stats);
      saveStatsToDisk(releaseName, { locales: mergedStats });
      process.exit(0);
    });
} catch (e) {
  console.error(e.message);
  process.exit(1);
}
