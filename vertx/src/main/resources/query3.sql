SELECT tweet_id, text, censored_text, impact_score
  FROM topic_word
  WHERE (user_id BETWEEN ? AND ?)
  AND (created_at BETWEEN ? AND ?)