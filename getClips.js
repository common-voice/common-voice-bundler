const fs = require('fs');
const path = require('path');
const readline = require('readline');
const csv = require('fast-csv');
const config = require('./config');

const {
  hashId,
  objectMap,
  mkDirByPathSync
} = require('./helpers');

const TSV_OPTIONS = {
  headers: true,
  delimiter: '\t',
  quote: false
};

const QUERY_FILE = path.join(__dirname, 'queries', config.get('queryFile'));
const OUT_DIR = config.get('localOutDir');
const TSV_PATH = path.join(OUT_DIR, 'clips.tsv');
const { name: CLIP_BUCKET_NAME } = config.get('clipBucket');

const processAndDownloadClips = (db, clipBucket) => {
  db.connect();

  return new Promise(resolve => {
    let activeDownloads = 0;
    let rowIndex = 0;
    let clipSavedIndex = 0;
    let readAllRows = false;
    const stats = {};

    const tsvStream = csv.createWriteStream(TSV_OPTIONS);

    if (!config.get('skipHashing')) {
      tsvStream.pipe(fs.createWriteStream(TSV_PATH));
    }

    const renderProgress = () => {
      readline.cursorTo(process.stdout, 0);
      process.stdout.write(
        rowIndex + ' rows processed, ' + clipSavedIndex + ' clips downloaded'
      );
    };

    const updateStats = (stats, row) => {
      const localeStats =
        stats[row.locale] ||
        (stats[row.locale] = {
          clips: 0,
          splits: { accent: {}, age: {}, gender: {} },
          usersSet: new Set()
        });

      localeStats.clips++;
      localeStats.usersSet.add(row.client_id);

      const { splits } = localeStats;

      for (const key of Object.keys(splits).filter(key => key != 'filter')) {
        const value = row[key];
        splits[key][value] = (splits[key][value] || 0) + 1;
      }
    };

    const formatFinalStats = (localeSplits) => {
      return objectMap(localeSplits, ({ clips, splits, usersSet }) => ({
        clips,
        splits: objectMap(splits, values =>
          objectMap(values, value => Number((value / clips).toFixed(2)))
        ),
        users: usersSet.size
      }));
    };

    const downloadClipFile = (path) => {
      return clipBucket.getObject({
        Bucket: CLIP_BUCKET_NAME,
        Key: path
      });
    };

    const cleanUp = () => {
      if (readAllRows && activeDownloads == 0) {
        db.end();
        console.log('');
        resolve(formatFinalStats(stats));
      }
    };

    db.query(fs.readFileSync(QUERY_FILE, 'utf-8'))
      .on('result', row => {
        rowIndex++;
        renderProgress();
        updateStats(stats, row);

        const clipsDir = path.join(OUT_DIR, row.locale, 'clips');
        const newPath = `common_voice_${row.locale}_${row.id}.mp3`;
        const soundFilePath = path.join(clipsDir, newPath);

        tsvStream.write({
          ...row,
          sentence: row.sentence.split('\r').join(' '),
          client_id: config.get('skipHashing') ? row.client_id : hashId(row.client_id),
          path: newPath
        });

        if (fs.existsSync(soundFilePath)) {
          return;
        }

        if (activeDownloads > 50) {
          db.pause();
        }

        activeDownloads++;

        mkDirByPathSync(clipsDir);
        downloadClipFile(row.path)
          .createReadStream()
          .pipe(fs.createWriteStream(soundFilePath))
          .on('finish', () => {
            activeDownloads--;
            if (activeDownloads < 25) {
              db.resume();
            }

            clipSavedIndex++;
            renderProgress();
            cleanUp();
          });
      })
      .on('end', () => {
        readAllRows = true;
        tsvStream.end();
        cleanUp();
      });
  });
};

module.exports = {
  processAndDownloadClips
}