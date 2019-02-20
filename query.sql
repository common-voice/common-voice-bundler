SELECT clips.client_id,
       path,
       REPLACE(sentence, '\r\n', ' ') AS sentence,
       COALESCE(SUM(votes.is_valid), 0) AS up_votes,
       COALESCE(SUM(NOT votes.is_valid), 0) AS down_votes,
       COALESCE(age, '') AS age,
       COALESCE(gender, '') AS gender,
       COALESCE(accents.accent, '') AS accent,
       locales.name AS locale,
       buckets.bucket
FROM clips
     LEFT JOIN votes ON clips.id = votes.clip_id
     LEFT JOIN user_clients ON clips.client_id = user_clients.client_id
     LEFT JOIN user_client_accents accents
               ON user_clients.client_id = accents.client_id AND
                  accents.locale_id = clips.locale_id
     LEFT JOIN locales ON clips.locale_id = locales.id
     LEFT JOIN user_client_locale_buckets buckets
               ON clips.locale_id = buckets.locale_id AND
                  clips.client_id = buckets.client_id
WHERE clips.locale_id NOT IN (SELECT id FROM locales WHERE name = 'fa')
GROUP BY clips.id
