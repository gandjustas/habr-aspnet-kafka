-- Публикация для нагрузочного теста логической репликации: слот он создаёт сам на каждый прогон
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'load_test_pub') THEN
        CREATE PUBLICATION load_test_pub FOR TABLE messages;
    END IF;
END $$;
