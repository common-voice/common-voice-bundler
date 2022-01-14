SELECT clips.id,
  clips.client_id,
  path,
  REPLACE(sentence, '\r\n', ' ') AS sentence,
  COALESCE(SUM(votes.is_valid), 0) AS up_votes,
  COALESCE(SUM(NOT votes.is_valid), 0) AS down_votes,
  COALESCE(age, '') AS age,
  COALESCE(gender, '') AS gender,
  client_accent_list.accent_list as accents,
  locales.name AS locale,
  COALESCE(terms.term_name, '') AS segment
FROM clips
  LEFT JOIN votes ON clips.id = votes.clip_id
  LEFT JOIN user_client_accents accents ON clips.client_id = accents.client_id
  AND accents.locale_id = clips.locale_id
  -- 	make list of individual users' accents
  JOIN (
    SELECT uc.client_id,
      GROUP_CONCAT(a.accent_token) as accent_list
    FROM user_clients uc
      JOIN user_client_accents uca ON uc.client_id = uca.client_id
      JOIN accents a ON uca.accent_id = a.id
    WHERE a.accent_token != 'unspecified'
  ) client_accent_list ON accents.client_id = client_accent_list.client_id
  LEFT JOIN locales ON clips.locale_id = locales.id
  -- A subquery for taxonomies is faster than a full join
  LEFT JOIN (
    SELECT sentence_id,
      term_name
    FROM taxonomy_entries
      INNER JOIN taxonomy_terms ON taxonomy_entries.term_id = taxonomy_terms.id
  ) terms ON clips.original_sentence_id = terms.sentence_id
  -- A subquery for demographics is faster than a full join
  LEFT JOIN (
    SELECT clip_demographics.clip_id,
      ages.age,
      genders.gender
    FROM clip_demographics
      LEFT JOIN demographics ON clip_demographics.demographic_id = demographics.id
      LEFT JOIN ages ON demographics.age_id = ages.id
      LEFT JOIN genders ON demographics.gender_id = genders.id
  ) demographics ON clips.id = demographics.clip_id
WHERE clips.created_at <= ?
GROUP BY clips.id