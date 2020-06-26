const convict = require('convict');

const config = convict({
  db: {
    host: {
      format: String,
      default: 'localhost',
    },
    user: {
      format: String,
      default: 'root',
    },
    password: {
      format: String,
      sensitive: true,
      default: 'root',
    },
    database: {
      format: String,
      default: 'voice',
    },
  },
  clipBucket: {
    name: {
      format: String,
      default: '',
    },
    region: {
      format: String,
      default: 'us-west-2',
    },
    accessKeyId: {
      format: String,
      sensitive: true,
      default: '',
    },
    secretAccessKey: {
      format: String,
      sensitive: true,
      default: '',
    },
  },
  outBucket: {
    name: {
      format: String,
      default: '',
    },
    accessKeyId: {
      format: String,
      sensitive: true,
      default: '',
    },
    secretAccessKey: {
      format: String,
      sensitive: true,
      default: '',
    },
    region: {
      format: String,
      default: 'us-west-2',
    },
  },
  releaseName: {
    format: String,
    default: 'cv-corpus-1',
  },
  skipBundling: {
    format: Boolean,
    default: false,
  },
  skipCorpora: {
    format: Boolean,
    default: false,
  },
  skipHashing: {
    format: Boolean,
    default: false,
  },
  skipDownload: {
    format: Boolean,
    default: false,
  },
  skipMinorityCheck: {
    format: Boolean,
    default: false,
  },
  skipReportedSentences: {
    format: Boolean,
    default: false,
  },
  startFromCorpora: {
    format: Boolean,
    default: false
  },
  queryFile: {
    format: String,
    default: 'bundleAll.sql',
  },
  singleBundle: {
    format: Boolean,
    default: false,
  },
});

config.loadFile('./config.json');
config.validate();

module.exports = config;
