require('dotenv').config();
const fs = require('fs');
const path = require('path');
const mysql = require('mysql');
const S3 = require('aws-sdk/clients/s3');
const config = require('./config');
const { mkDirByPathSync } = require('./helpers');

/**
 * Verify that local files are set up
 *
 * @param {string}    queryFile  query file location
 * @param {string}    clipsDir   release clips folder
 *
 * Throw error if queryFile doesn't exist, create folder if clipsDir doesn't exist
 */
function verifyFiles(queryFile, clipsDir) {
  if (!fs.existsSync(queryFile)) {
    throw new Error(`The query file specified at ${queryFile} does not exist`);
  }

  if (!fs.existsSync(clipsDir)) {
    mkDirByPathSync(clipsDir);
  }
}

/**
 * Connect to remote DB given config values
 *
 * @return {Object}   mysql db connection
 */
function connectToDb() {
  const {
    host, user, password, database,
  } = config.get('db');

  try {
    const db = mysql.createConnection({
      host: process.env.MYSQL_HOST || host,
      user: process.env.MYSQL_USER || user,
      password: process.env.MYSQL_PASS || password,
      database: process.env.MYSQL_DB || database,
    });

    return db;
  } catch (e) {
    throw new Error(
      `An error occurred while trying to connect to the database: ${e.message}`,
    );
  }
}

/**
 * Connect to S3 instance given options
 *
 * @param {Object}   options for connecting to a bucket (see config)
 *
 * Throw error if bucket authentication fails
 *
 * @return {Object}  object with name of bucket and bucket connection itself
 */
function initS3Bucket(bucketOpts) {
  const {
    accessKeyId, secretAccessKey, name, region,
  } = bucketOpts;

  const bucket = new S3({
    ...(accessKeyId
      ? {
        credentials: {
          accessKeyId,
          secretAccessKey,
        },
      }
      : {}),
    region,
  });

  bucket.headBucket({ Bucket: name }, (err) => {
    if (err) {
      throw new Error(
        `An error occurred trying to connect to S3 instance ${name}`,
      );
    }
  });

  return { name, bucket };
}

/**
 * Initialize external connections needed
 *
 * Verify that queryFile exists
 * Create a folder for the release if it doesn't already exist
 * Create a db connection
 * Verify that it's possible to connect to the clips and dataset buckets
 *
 * Exit with error if any of this fails
 * @return {Object}  objects for db, clip bucket, dataset bucket
 */
function initialize() {
  const releaseName = config.get('releaseName');
  const queryFile = path.join(__dirname, 'queries', config.get('queryFile'));

  try {
    verifyFiles(queryFile, releaseName);

    return {
      db: connectToDb(),
      clipBucket: initS3Bucket(config.get('clipBucket')),
      bundlerBucket: initS3Bucket(config.get('outBucket')),
    };
  } catch (e) {
    console.error(e);
    process.exit(1);
    return {};
  }
}

module.exports = {
  initialize,
};
