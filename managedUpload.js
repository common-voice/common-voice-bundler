const config = require('./config');
const fs = require('fs');
const path = require('path');
const tar = require('tar');
const merge = require('lodash.merge');
const { PassThrough } = require('stream');
const { logProgress, bytesToSize } = require('./helpers');
const crypto = require('crypto');

const RELEASE_NAME = config.get('releaseName');
const SINGLE_BUNDLE = config.get('singleBundle');

const { name: OUT_BUCKET_NAME } = config.get('outBucket');

const tarAndUploadBundle = (paths, archiveName, releaseName, bundlerBucket) => {
  return new Promise(resolve => {
    const sizes = {};
    let tarSize = 0;

    const stream = new PassThrough();
    console.log('archiving & uploading', releaseName);

    const managedUpload = bundlerBucket.upload({
      Body: stream,
      Bucket: OUT_BUCKET_NAME,
      Key: archiveName,
      ACL: 'public-read'
    });
    logProgress(managedUpload);

    const filePath = path.join(releaseName, `${releaseName}.tar.gz`);
    const writeStream = fs.createWriteStream(filePath);

    writeStream.on('finish', () => {
      const hash = crypto.createHash('sha256');
      let checksum;

      fs.createReadStream(filePath)
        .pipe(stream)
        .on('data', (data) => {
          process.stdout.write(`creating checksum for ${archiveName}...\r`);
          hash.update(data, 'utf8');
        })
        .on('end', () => {
          checksum = hash.digest('hex');
          console.log(`checksum created for ${archiveName}: ${checksum}`);
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
          resolve({ size: ContentLength, checksum });
        })
        .catch(err => console.error(err));
    });

    return tar
      .c({ gzip: true }, paths)
      .on('data', (data) => {
        tarSize = tarSize + data.length;
        process.stdout.write(`archive size: ${bytesToSize(tarSize)}\r`);

        writeStream.write(data);
      }).on('end', () => {
        console.log('');
        writeStream.end();
      });
  });
}

const uploadDataset = (localeDirs, bundlerBucket, releaseName) => {
  if (SINGLE_BUNDLE) {
    const localeDirPaths = localeDirs.map((locale) => {
      return path.join(releaseName, locale);
    });

    const archiveName = `${releaseName}/${releaseName}.tar.gz`;
    return tarAndUploadBundle(localeDirPaths, archiveName, releaseName, bundlerBucket).then((metadata) => {
      return { 'overall': metadata };
    });

  } else {
    const bundlePromises = localeDirs.map((locale) => {
      const localeDir = path.join(releaseName, locale);
      const archiveName = `${releaseName}/${locale}.tar.gz`;

      return tarAndUploadBundle(localeDirPaths, archiveName, releaseName, bundlerBucket).then((metadata) => {
        return { [locale]: metadata };
      });
    });

    return Promise.all((bundleStats) => {
      return merge(...bundleStats)
    });
  }

}

module.exports = {
  uploadDataset
}