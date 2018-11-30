SELECT user2_id as user_id, tweet_text, intimacy_score,
  user2_screen_name user user_screen_name, user2_desc as user_desc, created_at FROM
  user_intimacy WHERE user1_id=?
  union
  SELECT user1_id as user_id, tweet_text, intimacy_score,
  user2_screen_name as user_screen_name, user2_desc as user_desc, created_at FROM
  user_intimacy WHERE user2_id=?
  ORDER BY user2_id ASC, created_at DESC