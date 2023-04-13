const fs = require('fs');
const path = require('path');
const { connectToDb } = require('./init');
const crypto = require('crypto');

const SENTENCES_PATH = path.join(__dirname, 'undecidedSentences.tsv');
const VOTES_PATH = path.join(__dirname, 'undecidedVotes.tsv');
const LOCALES_PATH = path.join(__dirname, 'locales.tsv');
const USER_CLIENTS_PATH = path.join(__dirname, 'user_clients.tsv');

const SENTENCE_SALT = '8hd3e8sddFSdfj';
const BATCH_SIZE = 50000;
const PROCESSED_BATCHES_FILE = 'processedBatches.tsv';

const HY_LOCALE = 'hy'; // in SC
const HY_AM_LOCALE = 'hy-AM'; // in CV

const db = connectToDb();

const INSERT_SENTENCE_QUERY =
  'INSERT INTO sentences (id, text, source, locale_id) VALUES ';
const INSERT_METADATA_QUERY =
  'INSERT INTO sentence_metadata(sentence_id, client_id) VALUES ';
const INSERT_VOTE_QUERY =
  'INSERT INTO sentence_votes (sentence_id, vote, client_id) VALUES ';

/**
 * Used to hash sentences in import-sentences.ts
 */
function hashSentence(str) {
  return crypto.createHmac('sha256', SENTENCE_SALT).update(str).digest('hex');
}

/**
 * Create sentence id hash over sentence and locale id
 */
function createSentenceId(sentence, localeId) {
  return hashSentence(`${localeId}:${sentence}`);
}

/**
 * Mapping should be from name to locale id
 */
const getLocaleMapping = () => {
  const data = fs.readFileSync(LOCALES_PATH, { encoding: 'utf-8' });
  const localesMap = new Map();

  data.split('\n').forEach((x) => {
    if (!x) return;
    const [id, name] = x.split('\t');
    localesMap.set(name, id);
  });

  return localesMap;
};

const buildSentenceValues = (sentenceId, text, source, localeId) => {
  return `(${sentenceId}, ${text}, ${source}, ${localeId})`;
};

const buildSentenceMetadatValues = (sentenceId, clientId) => {
  return `(${sentenceId}, NULLIF(${clientId}, ''))`;
};

const buildSentenceVoteValues = (sentenceId, vote, clientId) => {
  return `(${sentenceId}, ${vote}, NULLIF(${clientId}, ''))`;
};

const batchInsertData = async (insertQuery, values, prefix) => {
  let current = 0;
  let start = 0;
  const total = values.length;

  if (process.env.npm_config_retry) {
    // processedBatches.tsv might not exist yet
    try {
      start = parseInt(fs.readFileSync(prefix + PROCESSED_BATCHES_FILE));
    } catch (err) {
      start = 0;
    }
  }

  console.log(`Starting ${prefix}-batches from ${start}`);

  while (true) {
    const end = start + BATCH_SIZE;
    const batch = values.slice(start, end);

    if (batch.length === 0) break;

    const insertValues = batch.join(',');

    const query = insertQuery + insertValues;
    console.log(query.slice(0, 200));

    db.beginTransaction((err) => {
      if (err) {
        throw err;
      }

      db.query(query, (err, results, fields) => {
        if (err) {
          return db.rollback(() => {
            throw err;
          });
        }

        console.log(`Processed  ${results.affectedRows} rows.`);
      });

      db.commit((err) => {
        if (err) {
          return db.rollback(() => {
            throw err;
          });
        }

        current += batch.length;
        start = end;

        console.log(`Progress: ${current}/${total}\n\n`);
      });
    });

    fs.writeFileSync(prefix + PROCESSED_BATCHES_FILE, '' + current + '\n');

    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
};

const getUserClientsEmailToIdMap = () => {
  const userClients = fs.readFileSync(USER_CLIENTS_PATH, { encoding: 'utf-8' });
  const userClientsEmailToIdMap = new Map();
  userClients.split('\n').forEach((x) => {
    if (!x) return;
    const [id, email] = x.split('\t');
    userClientsEmailToIdMap.set(email, id);
  });
  return userClientsEmailToIdMap;
};

const getUpdatedSentencesMap = (
  sentences,
  userClientEmailMap,
  localeMapping
) => {
  const sentenceIdMap = new Map();

  sentences
    .split('\n')
    .slice(1)
    .forEach((x) => {
      if (!x) return;
      const [id, sentence, source, localeId, userEmail, ...rest] =
        x.split('\t');
      sentenceIdMap.set(id, {
        sentenceId: createSentenceId(sentence, localeId),
        text: sentence,
        source: source,
        localeId:
          localeId === HY_LOCALE
            ? localeMapping.get(HY_AM_LOCALE)
            : localeMapping.get(localeId),
        clientId: userClientEmailMap.get(userEmail) ?? '',
      });
    });
  return sentenceIdMap;
};

const getUpdatedVotesMap = (votes, sentencesMap, userClientEmailMap) => {
  const votesMap = new Map();

  votes
    .split('\n')
    .slice(1)
    .forEach((x) => {
      if (!x) return;

      const [id, approval, sentenceId, createdAt, updatedAt, userId] =
        x.split('\t');
      const sentence = sentencesMap.get(sentenceId);

      votesMap.set(id, {
        sentenceId: sentence.sentenceId,
        vote: approval,
        clientId: userClientEmailMap.get(userId) ?? '',
      });
    });
  return votesMap;
};

const run = async () => {
  const sentences = fs.readFileSync(SENTENCES_PATH, { encoding: 'utf-8' });
  const votes = fs.readFileSync(VOTES_PATH, { encoding: 'utf-8' });
  const userClientEmailMap = getUserClientsEmailToIdMap();
  const updatedSentencesMap = getUpdatedSentencesMap(
    sentences,
    userClientEmailMap,
    getLocaleMapping()
  );
  const votesMap = getUpdatedVotesMap(
    votes,
    updatedSentencesMap,
    userClientEmailMap
  );

  const sentenceValues = [];
  const metadataValues = [];
  const voteValues = [];

  for (const [id, sentence] of updatedSentencesMap) {
    sentenceValues.push(
      buildSentenceValues(
        sentence.sentenceId,
        sentence.text,
        sentence.source,
        sentence.localeId
      )
    );
    metadataValues.push(
      buildSentenceMetadatValues(sentence.id, sentence.clientId)
    );
  }

  for (const [id, vote] of votesMap) {
    voteValues.push(
      buildSentenceVoteValues(vote.sentenceId, vote.vote, vote.clientId)
    );
  }

  console.log('Start inserting sentences...');
  await batchInsertData(INSERT_SENTENCE_QUERY, sentenceValues, 'sentences-');
  console.log('Finished inserting sentences.');
  console.log('Start inserting sentences metadata...');
  await batchInsertData(INSERT_METADATA_QUERY, metadataValues, 'metadata-');
  console.log('Finished inserting sentences metadata.');
  console.log('Start inserting sentences vote...');
  await batchInsertData(INSERT_VOTE_QUERY, voteValues, 'votes-');
  console.log('Finished inserting sentences vote.');

  process.exit(0);
};

run();
