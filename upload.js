const fs = require('fs');
const readline = require('readline');
const path = require('path');
const tar = require('tar');
const merge = require('lodash.merge');
const { PassThrough } = require('stream');
const crypto = require('crypto');
const { bytesToSize, mkDirByPathSync, sequencePromises } = require('./helpers');
const { saveStatsToDisk } = require('./processStats');
const config = require('./config');
const SINGLE_BUNDLE = config.get('singleBundle');
const EXCLUDED_FILES = ['clips', 'dev.tsv', 'test.tsv', 'train.tsv'];

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

  fs.writeFileSync(uploadPath, JSON.stringify(diskData), 'utf8', (err) => {
    if (err) throw err;
    return true;
  });
};

/**
 * Helper function to see if current archive has already been zipped and uploaded
 *
 * @param {string} releaseName     name of current release
 * @param {string} labelName          the archive name
 *
 * @return {boolean} true if already uploaded
 */
const getMetadataIfProcessed = (releaseName, labelName) => {
  try {
    const data = fs.readFileSync(
      path.join(releaseName, 'uploaded.json'),
      'utf-8'
    );
    const label = labelName || releaseName;
    const uploaded = JSON.parse(data);
    return uploaded[label] ? { [label]: uploaded[label] } : null;
  } catch (e) {
    return false;
  }
};

const getPathFromLine = (clipsTsvLine) => {
  const values = clipsTsvLine.split('\t');
  return values.length <= 1 ? '' : values[2];
}

const isLineFromLocale = (clipsTsvLine, locale) => {
  const values = clipsTsvLine.split('\t');
  return values[10].trim() === locale;
}

const getClipList = async (releaseName, locale) => {
  const fileData = fs.createReadStream(path.join(releaseName, 'clips.tsv'), {
    encoding: 'utf8',
  });

  const fileNames = [];
  const rl = readline.createInterface(fileData);

  for await (const line of rl) {
    if (isLineFromLocale(line, locale)) {
      fileNames.push(getPathFromLine(line))
    }
  }

  return fileNames;
};

/**
 * Main function for zipping and uploading files - one archive per function run
 *
 * @param {array} clipsPaths       array of directories to include in tar
 * @param {string} releaseName     name of current release
 * @param {string} locale     name of archive - usually locale
 * @param {Object} bundlerBucket   datasets bucket object with name and bucket keys
 *
 * @return {Object} stats object
 */
const tarAndUploadBundle = (clipsPaths, releaseName, locale, bundlerBucket) =>
  new Promise((resolve) => {
    let tarSize = 0;

    const stream = new PassThrough();
    const fileName =
      locale && locale !== ''
        ? `${releaseName}-${locale}.tar.gz`
        : `${releaseName}.tar.gz`;
    const localArchiveDir = path.join(releaseName, 'tarballs');
    mkDirByPathSync(localArchiveDir);

    const localFilePath = path.join(localArchiveDir, `${fileName}`);
    const writeStream = fs.createWriteStream(localFilePath);
    const remoteArchiveKey = `${releaseName}/${fileName}`;

    const managedUpload = bundlerBucket.bucket
      .upload(
        {
          Body: stream,
          Bucket: bundlerBucket.name,
          Key: remoteArchiveKey,
          ACL: 'public-read',
        },
        {
          // max part size is 5gb, max # of objects is 10k
          partSize: 25 * 1024 * 1024,
        }
      )
      .on('httpUploadProgress', (progress) => {
        process.stdout.write(
          `uploading ${fileName}: ${bytesToSize(progress.loaded)}      \r`
        );
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
        .then(() =>
          bundlerBucket.bucket
            .headObject({ Bucket: bundlerBucket.name, Key: remoteArchiveKey })
            .promise()
        )
        .then(({ ContentLength }) => {
          console.log(`\n${fileName} uploaded`);
          const metadata = { size: ContentLength, checksum };
          addUploadToDisk(releaseName, locale, metadata);
          resolve(metadata);
        })
        .catch((err) => console.error(err));
    });

    // the part that actually does the tarring
    return tar
      .c({ gzip: true }, clipsPaths)
      .on('data', (data) => {
        tarSize += data.length;
        process.stdout.write(
          `archiving ${fileName}: ${bytesToSize(tarSize)}      \r`
        );
        writeStream.write(data);
      })
      .on('end', () => {
        console.log('');
        writeStream.end();
      });
  });

const getMetadataFiles = (localeDir) => {
  return fs
    .readdirSync(localeDir)
    .filter((fileName) => !EXCLUDED_FILES.includes(fileName));
};

/**
 * Entry point function for zipping and uploading files
 *
 * @param {array} locales          array of locales to process
 * @param {Object} bundlerBucket   datasets bucket object with name and bucket keys
 * @param {string} releaseName     name of current release
 *
 * @return {Object} stats object
 */
const uploadDataset = async (locales, bundlerBucket, releaseName) => {
  // If all languages are in a single bundle
  if (SINGLE_BUNDLE) {
    if (getMetadataIfProcessed(releaseName)) {
      return getMetadataIfProcessed(releaseName);
    }

    // If single bundle, all paths in release directory should be zipped
    const localeDirPaths = locales.map((locale) =>
      path.join(releaseName, locale)
    );

    return tarAndUploadBundle(
      localeDirPaths,
      releaseName,
      '',
      bundlerBucket
    ).then((metadata) => {
      // save stats to disk and return stats
      saveStatsToDisk(releaseName, { [releaseName]: metadata });
      return { [releaseName]: metadata };
    });
  }
  // Internal helper function to zip and upload a single locale in Promise format
  const tarLocale = async (locale) => {
    const labelName = `${releaseName}-${locale}`;
    const existingData = getMetadataIfProcessed(releaseName, locale);
    
    if (existingData) {
      console.log(
        `${labelName}.tar.gz of size ${bytesToSize(
          existingData[locale].size
        )} was previously uploaded with a checksum of ${existingData[locale].checksum
        }`
      );
      return new Promise((resolve) => resolve(existingData));
    }

    // for full releases, this is a path,
    const localePathToDir = path.join(releaseName, locale);
    // <releaseName>/<locale_token>
    let localeDir = [localePathToDir]; // array because tar.c needs it to be

    console.log('localePathToDir', localePathToDir);
    //only upload files in clips.tsv
    if (config.get('startCutoffTime')) {
      const metadataFiles = getMetadataFiles(localePathToDir).map((fileName) =>
        path.join(localePathToDir, fileName)
      );
      const clipList = await getClipList(releaseName, locale);
      localeDir = clipList.map((fileName) =>
        path.join(localePathToDir, 'clips', fileName)
      ); //for delta, this is an array of paths
      console.log(
        'Fetching all relevant clips for delta release: ',
        localeDir.length,
        'clips for ',
        locale
      );
      //join metadata paths with clip paths for uploading
      localeDir = localeDir.concat(metadataFiles);
      console.log('Concat metadata files:', metadataFiles.length);
    }

    return tarAndUploadBundle(
      localeDir,
      releaseName,
      locale,
      bundlerBucket
    ).then((metadata) => ({ [locale]: metadata }));
  };

  // Sequentially zip and upload all languages
  return sequencePromises(locales, [], tarLocale).then((stats) => {
    const mergedStats = merge(...stats);
    saveStatsToDisk(releaseName, { locales: mergedStats });
    return mergedStats;
  });
};

module.exports = {
  uploadDataset,
};
