const config = require('./config');
const fs = require('fs');
const path = require('path');
const tar = require('tar');
const { PassThrough } = require('stream');
const { logProgress } = require('./helpers');
const crypto = require('crypto');

const RELEASE_NAME = config.get('releaseName');
const { name: OUT_BUCKET_NAME } = config.get('outBucket');

const uploadDataset = (localeDirs, bundlerBucket, releaseName) => {
  return localeDirs.reduce((promise, locale) => {
    return promise.then(sizes => {
      const stream = new PassThrough();
      const archiveName = `${releaseName}/${locale}.tar.gz`;
      console.log('archiving & uploading', archiveName);

      const managedUpload = bundlerBucket.upload({
        Body: stream,
        Bucket: OUT_BUCKET_NAME,
        Key: archiveName,
        ACL: 'public-read'
      });
      logProgress(managedUpload);

      const localeDir = path.join(RELEASE_NAME, locale);

      const filePath = path.join(__dirname, releaseName, `${locale}.tar.gz`);

      return tar
        .c({ gzip: true, cwd: localeDir, file: filePath }, fs.readdirSync(localeDir))
        .then(() => {
          const hash = crypto.createHash('sha256');
          let checksum;

          fs.createReadStream(filePath)
            .pipe(stream)
            .on('data', (data) => {
              hash.update(data, 'utf8');
            })
            .on('end', () => {
              checksum = hash.digest('hex');
            });

          return managedUpload
            .promise()
            .then(() =>
              bundlerBucket
                .headObject({ Bucket: OUT_BUCKET_NAME, Key: archiveName })
                .promise()
            )
            .then(({ ContentLength }) => {
              console.log('');
              sizes[locale] = { size: ContentLength, checksum };
              return sizes;
            })
            .catch(err => console.error(err));
        })

    });
  }, Promise.resolve({}));
}

module.exports = {
  uploadDataset
}