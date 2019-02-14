const convict = require('convict');

const config = convict({
  db: {
    host: {
      format: String,
      default: 'localhost'
    },
    user: {
      format: String,
      default: 'root'
    },
    password: {
      format: String,
      sensitive: true,
      default: 'root'
    },
    database: {
      format: String,
      default: 'voice'
    }
  },
  clipBucket: {
    name: {
      format: String,
      default: ''
    },
    region: {
      format: String,
      default: 'us-west-2'
    },
    accessKeyId: {
      format: String,
      sensitive: true,
      default: ''
    },
    secretAccessKey: {
      format: String,
      sensitive: true,
      default: ''
    }
  },
  outBucket: {
    name: {
      format: String,
      default: ''
    },
    accessKeyId: {
      format: String,
      sensitive: true,
      default: ''
    },
    secretAccessKey: {
      format: String,
      sensitive: true,
      default: ''
    }
  },
  salt: {
    format: String,
    sensitive: true,
    default: ''
  },
  skipBundling: {
    format: Boolean,
    default: false
  }
});

config.loadFile('./config.json');
config.validate();

module.exports = config;
