require('dotenv').config();
const fs = require('fs');
const path = require('path');
const mysql = require('mysql');
const S3 = require('aws-sdk/clients/s3');
const config = require('./config');
const { mkDirByPathSync } = require('./helpers');

function verifyFiles(queryFile, clipsDir) {
  if (!fs.existsSync(queryFile)) {
    throw new Error(`The query file specified at ${queryFile} does not exist`);
  }

  if (!fs.existsSync(clipsDir)) {
    mkDirByPathSync(clipsDir);
  }
}

function connectToDb() {
  const { host, user, password, database } = config.get('db');

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
      `An error occurred while trying to connect to the database: ${e.message}`
    );
  }
}

function initS3Bucket(bucketOpts) {
  const { accessKeyId, secretAccessKey, name, region } = bucketOpts;

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
        `An error occurred trying to connect to S3 instance ${name}`
      );
    }
  });

  return bucket;
}

function initialize() {
  const RELEASE_NAME = config.get('releaseName');
  const QUERY_FILE = path.join(__dirname, 'queries', config.get('queryFile'));

  try {
    verifyFiles(QUERY_FILE, RELEASE_NAME);

    return {
      db: connectToDb(),
      clipBucket: initS3Bucket(config.get('clipBucket')),
      bundlerBucket: initS3Bucket(config.get('outBucket')),
    };
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
}

module.exports = {
  initialize
};