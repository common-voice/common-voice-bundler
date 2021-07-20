const fs = require('fs');
const path = require('path');
const tar = require('tar');
const merge = require('lodash.merge');
const { PassThrough } = require('stream');
const crypto = require('crypto');
const { bytesToSize, mkDirByPathSync, sequencePromises } = require('./helpers');
const { saveStatsToDisk } = require('./processStats');
const config = require('./config');

const SINGLE_BUNDLE = config.get('singleBundle');

/**
 * Helper function to log a fully uploaded tar to disk so it can be skipped next time
 *
 * @param {string} releaseName     name of current release
 * @param {string} label           the archive name, usually locale
 * @param {Object} data            object with file size and checksum of archive
 *
 * @return {boolean} true if logging succeeded
 */
const addUploadToDisk = (releaseName, label, data) => {
  const uploadPath = path.join(releaseName, 'uploaded.json');
  let diskData = {};

  if (fs.existsSync(uploadPath)) {
    diskData = JSON.parse(fs.readFileSync(uploadPath, 'utf-8'));
  }

  diskData[label] = data;

  fs.writeFileSync(uploadPath, JSON.stringify(diskData), 'utf8', (
    err,
  ) => {
    if (err) throw err;
    return true;
  });
};

/**
 * Helper function to see if current archive has alreay been zipped and uploaded
 *
 * @param {string} releaseName     name of current release
 * @param {string} locale          the archive name
 *
 * @return {boolean} true if already uploaded
 */
const checkIfProcessed = (releaseName, locale) => {
  try {
    const data = fs.readFileSync(path.join(releaseName, 'uploaded.json'), 'utf-8');
    const label = locale || releaseName;
    const uploaded = JSON.parse(data);
    if (uploaded[label]) {
      console.log(`${label}.tar.gz of size ${bytesToSize(uploaded[label].size)} was previously uploaded with a checksum of ${uploaded[label].checksum}`);
    }
    return uploaded[label];
  } catch (e) {
    return false;
  }
};

/**
 * Helper function to get archived filesize and checksum data from disk
 *
 * @param {string} releaseName         name of current release
 * @param {string} locale (optional)   the locale to check for
 */
const getUploadedDataFromDisk = (releaseName, locale) => {
  // if no locale provided, it's a single bundle file, so use releaseName for label too
  const label = locale || releaseName;
  const uploaded = JSON.parse(
    fs.readFileSync(path.join(releaseName, 'uploaded.json'), 'utf-8'),
  );
  return { [label]: uploaded[label] };
};

/**
 * Main function for zipping and uploading files - one archive per function run
 *
 * @param {array} clipsPaths       array of directories to include in tar
 * @param {string} releaseName     name of current release
 * @param {string} archiveLabel     name of archive - usually locale
 * @param {Object} bundlerBucket   datasets bucket object with name and bucket keys
 *
 * @return {Object} stats object
 */
const tarAndUploadBundle = (
  clipsPaths,
  releaseName,
  archiveLabel,
  bundlerBucket,
) => new Promise((resolve) => {
  let tarSize = 0;

  const stream = new PassThrough();
  const fileName = `${archiveLabel}.tar.gz`;
  const localArchiveDir = path.join(releaseName, 'tarballs');
  mkDirByPathSync(localArchiveDir);

  const localFilePath = path.join(localArchiveDir, `${fileName}`);
  const writeStream = fs.createWriteStream(localFilePath);
  const remoteArchiveKey = `${releaseName}/${fileName}`;

  const managedUpload = bundlerBucket.bucket.upload({
    Body: stream,
    Bucket: bundlerBucket.name,
    Key: remoteArchiveKey,
    ACL: 'public-read',
  }, {
    // max part size is 5gb, max # of objects is 10k
    partSize: 25 * 1024 * 1024,
  }).on('httpUploadProgress', (progress) => {
    process.stdout.write(`uploading ${fileName}: ${bytesToSize(progress.loaded)}      \r`);
  });

  // upon tar completion, create checksum and upload
  writeStream.on('finish', () => {
    const hash = crypto.createHash('sha256');
    let checksum;
    console.log(`creating checksum for ${fileName}...`);

    fs.createReadStream(localFilePath)
      .pipe(stream)
      .on('data', (data) => {
        hash.update(data, 'utf8');
      })
      .on('end', () => {
        checksum = hash.digest('hex');
        console.log(`checksum created for ${fileName}: ${checksum}`);
      });

    return managedUpload
      .promise()
      .then(() => bundlerBucket.bucket
        .headObject({ Bucket: bundlerBucket.name, Key: remoteArchiveKey })
        .promise())
      .then(({ ContentLength }) => {
        console.log(`\n${fileName} uploaded`);
        const metadata = { size: ContentLength, checksum };
        addUploadToDisk(releaseName, archiveLabel, metadata);
        resolve(metadata);
      })
      .catch((err) => console.error(err));
  });

  // the part that actually does the tarring
  return tar
    .c({ gzip: true }, clipsPaths)
    .on('data', (data) => {
      tarSize += data.length;
      process.stdout.write(`archiving ${fileName}: ${bytesToSize(tarSize)}      \r`);
      writeStream.write(data);
    })
    .on('end', () => {
      console.log('');
      writeStream.end();
    });
});

/**
 * Entry point function for zipping and uploading files
 *
 * @param {array} locales          array of locales to process
 * @param {Object} bundlerBucket   datasets bucket object with name and bucket keys
 * @param {string} releaseName     name of current release
 *
 * @return {Object} stats object
 */
const uploadDataset = (locales, bundlerBucket, releaseName) => {
  // If all languages are in a single bundle
  if (SINGLE_BUNDLE) {
    if (checkIfProcessed(releaseName)) {
      return getUploadedDataFromDisk(releaseName);
    }

    // If single bundle, all paths in release directory should be zipped
    const localeDirPaths = locales.map((locale) => path.join(releaseName, locale));

    return tarAndUploadBundle(
      localeDirPaths,
      releaseName,
      releaseName,
      bundlerBucket,
    ).then((metadata) => {
      // save stats to disk and return stats
      saveStatsToDisk(releaseName, { [releaseName]: metadata });
      return { [releaseName]: metadata };
    });
  }
  // Internal helper function to zip and upload a single locale in Promise format
  const tarLocale = (locale) => {
    const localeDir = path.join(releaseName, locale);

    if (checkIfProcessed(releaseName, locale)) {
      return new Promise((resolve) => resolve(getUploadedDataFromDisk(releaseName, locale)));
    }

    return tarAndUploadBundle(
      [localeDir],
      releaseName,
      `${releaseName}-${locale}`,
      bundlerBucket,
    ).then((metadata) => ({ [locale]: metadata }));
  };

  // Sequentially zip and upload all languages
  return sequencePromises(locales, [], tarLocale)
    .then((stats) => {
      const mergedStats = merge(...stats);
      saveStatsToDisk(releaseName, { locales: mergedStats });
      return mergedStats;
    });
};

module.exports = {
  uploadDataset,
};
