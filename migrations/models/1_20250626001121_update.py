from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE UNIQUE INDEX "uid_file_live_id_93a836" ON "file" ("live_id", "file_folder", "file_name");
        CREATE UNIQUE INDEX "uid_rawfile_file_id_bedae0" ON "rawfile" ("file_id", "segment_idx");
        CREATE UNIQUE INDEX "uid_rawfile_chat_id_42bbd0" ON "rawfile" ("chat_id", "message_id");"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        DROP INDEX IF EXISTS "uid_rawfile_chat_id_42bbd0";
        DROP INDEX IF EXISTS "uid_rawfile_file_id_bedae0";
        DROP INDEX IF EXISTS "uid_file_live_id_93a836";"""
