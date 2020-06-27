const config = require('./config');
const fs = require('fs');
const path = require('path');
const tar = require('tar');
const merge = require('lodash.merge');
const { PassThrough } = require('stream');
const { bytesToSize, mkDirByPathSync, sequencePromises } = require('./helpers');
const { saveStatsToDisk } = require('./processStats');
const crypto = require('crypto');

const SINGLE_BUNDLE = config.get('singleBundle');

const addUploadToDisk = (releaseName, label, data) => {
  const uploadPath = path.join(releaseName, 'uploaded.json');
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
  try {
    const data = fs.readFileSync(path.join(releaseName, 'uploaded.json'), 'utf-8')
    const label = locale || releaseName;
    const uploaded = JSON.parse(data);
    if (uploaded[label]) {
      console.log(`${label}.tar.gz of size ${bytesToSize(uploaded[label].size)} was previously uploaded with a checksum of ${uploaded[label].checksum}`);
    }
    return uploaded[label];
  } catch(e) {
    return false;
  }
};

const logProgress = (managedUpload, fileName) => {
  managedUpload.on('httpUploadProgress', progress => {
    process.stdout.write(`uploading ${fileName}: ${bytesToSize(progress.loaded)}      \r`);
  });
}

const getUploadedDataFromDisk = (releaseName, locale) => {
  const label = locale || releaseName;
  const uploaded = JSON.parse(
    fs.readFileSync(path.join(releaseName, 'uploaded.json'), 'utf-8')
  );
  return { [label]: uploaded[label] };
};

const tarAndUploadBundle = (
  clipsPaths,
  releaseName,
  archiveLabel,
  bundlerBucket
) => {
  return new Promise(resolve => {
    let tarSize = 0;

    const stream = new PassThrough();
    const fileName = `${archiveLabel}.tar.gz`;
    const localArchiveDir = path.join(releaseName, 'tarballs');
    const remoteArchiveKey = `${releaseName}/${fileName}`;

    mkDirByPathSync(localArchiveDir);

    const managedUpload = bundlerBucket.bucket.upload({
      Body: stream,
      Bucket: bundlerBucket.name,
      Key: remoteArchiveKey,
      ACL: 'public-read',
    });
    logProgress(managedUpload, fileName);

    const localFilePath = path.join(localArchiveDir, `${fileName}`);
    const writeStream = fs.createWriteStream(localFilePath);

    writeStream.on('finish', () => {
      const hash = crypto.createHash('sha256');
      let checksum;
      console.log(`creating checksum for ${fileName}...`);

      fs.createReadStream(localFilePath)
        .pipe(stream)
        .on('data', data => {
          hash.update(data, 'utf8');
        })
        .on('end', () => {
          checksum = hash.digest('hex');
          console.log(`checksum created for ${fileName}: ${checksum}`);
        });

      return managedUpload
        .promise()
        .then(() =>
          bundlerBucket.bucket
            .headObject({ Bucket: bundlerBucket.name, Key: remoteArchiveKey })
            .promise()
        )
        .then(({ ContentLength }) => {
          console.log(`\n${fileName} uploaded`);
          const metadata = { size: ContentLength, checksum };
          addUploadToDisk(releaseName, archiveLabel, metadata);
          resolve(metadata);
        })
        .catch(err => console.error(err));
    });

    return tar
      .c({ gzip: true }, clipsPaths)
      .on('data', data => {
        tarSize = tarSize + data.length;
        process.stdout.write(`archiving ${fileName}: ${bytesToSize(tarSize)}      \r`);
        writeStream.write(data);
      })
      .on('end', () => {
        console.log('');
        writeStream.end();
      });
  });
};

const uploadDataset = (locales, bundlerBucket, releaseName) => {
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
      saveStatsToDisk(releaseName, { [releaseName]: metadata });
      return { [releaseName]: metadata };
    });
  } else {
    const tarLocale = (locale) => {
      const localeDir = path.join(releaseName, locale);

      if (checkIfProcessed(releaseName, locale)) {
        return new Promise(resolve => resolve(getUploadedDataFromDisk(releaseName, locale)));
      }

      return tarAndUploadBundle(
        [localeDir],
        releaseName,
        locale,
        bundlerBucket
      ).then(metadata => {
        return({ [locale]: metadata });
      });
    }

    return sequencePromises(locales, [], tarLocale)
      .then(stats => {
        const mergedStats = merge(...stats)
        saveStatsToDisk(releaseName, mergedStats);
        return mergedStats;
    });

  }
};

module.exports = {
  uploadDataset,
};
