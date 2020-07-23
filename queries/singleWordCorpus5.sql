SELECT clips.id,
       clips.client_id,
       path,
       REPLACE(sentence, '\r\n', ' ') AS sentence,
       COALESCE(SUM(votes.is_valid), 0) AS up_votes,
       COALESCE(SUM(NOT votes.is_valid), 0) AS down_votes,
       COALESCE(age, '') AS age,
       COALESCE(gender, '') AS gender,
       COALESCE(accents.accent, '') AS accent,
       locales.name AS locale,
       COALESCE(taxonomy_terms.term_name, '') AS segment
FROM clips
     LEFT JOIN votes ON clips.id = votes.clip_id
     LEFT JOIN taxonomy_entries ON taxonomy_entries.sentence_id = clips.original_sentence_id
     LEFT JOIN taxonomy_terms ON taxonomy_entries.term_id = taxonomy_terms.id
     LEFT JOIN user_clients ON clips.client_id = user_clients.client_id
     LEFT JOIN user_client_accents accents
               ON user_clients.client_id = accents.client_id AND
                  accents.locale_id = clips.locale_id
     LEFT JOIN locales ON clips.locale_id = locales.id
WHERE clips.created_at <= "2020-06-22 23:59:59"
AND taxonomy_entries.term_id = 1
GROUP BY clips.id