-- Публикация для PgOutputConsumerService: слот rep_slot создаётся приложением на старте
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'rep_pub') THEN
        CREATE PUBLICATION rep_pub FOR TABLE messages;
    END IF;
END $$;
