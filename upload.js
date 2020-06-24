const config = require('./config');
const fs = require('fs');
const path = require('path');
const tar = require('tar');
const merge = require('lodash.merge');
const { PassThrough } = require('stream');
const { logProgress, bytesToSize } = require('./helpers');
const crypto = require('crypto');

const SINGLE_BUNDLE = config.get('singleBundle');

const addUploadToDisk = (releaseName, locale, data) => {
  const uploadPath = path.join(releaseName, 'uploaded.json');
  const label = locale || releaseName;
  let diskData = {};

  if (fs.existsSync(uploadPath)) {
    diskData = JSON.parse(fs.readFileSync(uploadPath, 'utf-8'));
  }

  diskData[label] = data;

  fs.writeFileSync(uploadPath, JSON.stringify(diskData), 'utf8', function (
    err
  ) {
    if (err) throw err;
    return true;
  });
};

const checkIfProcessed = (releaseName, locale) => {
  return fs.readFile(
    path.join(releaseName, 'uploaded.json'),
    'utf-8',
    (err, data) => {
      if (err) return false;

      const label = locale || releaseName;
      const uploaded = JSON.parse(data);
      return uploaded[label];
    }
  );
};

const getUploadedDataFromDisk = (releaseName, locale) => {
  const label = locale || releaseName;
  const uploaded = JSON.parse(
    fs.readFileSync(path.join(releaseName, 'uploaded.json'), 'utf-8')
  );
  return { [label]: uploaded[label] };
};

const tarAndUploadBundle = (
  clipsPaths,
  archiveLabel,
  releaseName,
  bundlerBucket
) => {
  return new Promise(resolve => {
    let tarSize = 0;

    const stream = new PassThrough();
    const archivePath = `${releaseName}/${archiveLabel}.tar.gz`;

    console.log('archiving & uploading', archivePath);

    const managedUpload = bundlerBucket.bucket.upload({
      Body: stream,
      Bucket: bundlerBucket.name,
      Key: archivePath,
      ACL: 'public-read',
    });
    logProgress(managedUpload);

    const filePath = path.join(
      releaseName,
      'tarballs',
      `${releaseName}.tar.gz`
    );
    const writeStream = fs.createWriteStream(filePath);

    writeStream.on('finish', () => {
      const hash = crypto.createHash('sha256');
      let checksum;
      console.log(`creating checksum for ${archiveName}...`);

      fs.createReadStream(filePath)
        .pipe(stream)
        .on('data', data => {
          hash.update(data, 'utf8');
        })
        .on('end', () => {
          checksum = hash.digest('hex');
          console.log(`checksum created for ${archiveName}: ${checksum}`);
        });

      return managedUpload
        .promise()
        .then(() =>
          bundlerBucket.bucket
            .headObject({ Bucket: bundlerBucket.name, Key: archiveName })
            .promise()
        )
        .then(({ ContentLength }) => {
          console.log(`${archiveName} uploaded`);
          addUploadToDisk(releaseName, locale);
          resolve({ size: ContentLength, checksum });
        })
        .catch(err => console.error(err));
    });

    return tar
      .c({ gzip: true }, clipsPaths)
      .on('data', data => {
        tarSize = tarSize + data.length;
        process.stdout.write(`archive size: ${bytesToSize(tarSize)}      \r`);
        writeStream.write(data);
      })
      .on('end', () => {
        console.log('');
        writeStream.end();
      });
  });
};

const uploadDataset = (locales, releaseName, bundlerBucket) => {
  if (SINGLE_BUNDLE) {
    if (checkIfProcessed(releaseName)) {
      return getUploadedDataFromDisk(releaseName);
    }

    const localeDirPaths = locales.map(locale => {
      return path.join(releaseName, locale);
    });

    return tarAndUploadBundle(
      localeDirPaths,
      releaseName,
      releaseName,
      bundlerBucket
    ).then(metadata => {
      return { [releaseName]: metadata };
    });
  } else {
    const bundlePromises = locales.map(locale => {
      const localeDir = path.join(releaseName, locale);

      if (checkIfProcessed(releaseName, locale)) {
        return getUploadedDataFromDisk(releaseName, locale);
      }

      return tarAndUploadBundle(
        localeDirPaths,
        archiveName,
        locale,
        bundlerBucket
      ).then(metadata => {
        return { [locale]: metadata };
      });
    });

    return Promise.all(bundleStats => {
      return merge(...bundleStats);
    });
  }
};

module.exports = {
  uploadDataset,
};
