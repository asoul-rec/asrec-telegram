from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE IF NOT EXISTS "live" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "raw_name" VARCHAR(255) NOT NULL UNIQUE,
    "title" VARCHAR(255) NOT NULL,
    "artist" VARCHAR(20) NOT NULL,
    "start_time" TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS "file" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "total_segments" INT NOT NULL,
    "size" INT NOT NULL,
    "file_folder" VARCHAR(255) NOT NULL,
    "file_name" VARCHAR(255) NOT NULL,
    "mediainfo" JSON NOT NULL,
    "live_id" INT NOT NULL REFERENCES "live" ("id") ON DELETE CASCADE,
    CONSTRAINT "uid_file_live_id_93a836" UNIQUE ("live_id", "file_folder", "file_name")
);
CREATE TABLE IF NOT EXISTS "rawfile" (
    "file_unique_id" VARCHAR(255) NOT NULL PRIMARY KEY,
    "segment_idx" INT NOT NULL,
    "size" INT NOT NULL,
    "offset" INT NOT NULL,
    "hash" VARCHAR(255),
    "chat_id" INT NOT NULL,
    "message_id" INT NOT NULL,
    "file_id" INT NOT NULL REFERENCES "file" ("id") ON DELETE CASCADE,
    CONSTRAINT "uid_rawfile_file_id_bedae0" UNIQUE ("file_id", "segment_idx"),
    CONSTRAINT "uid_rawfile_chat_id_42bbd0" UNIQUE ("chat_id", "message_id")
);
CREATE TABLE IF NOT EXISTS "aerich" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "version" VARCHAR(255) NOT NULL,
    "app" VARCHAR(100) NOT NULL,
    "content" JSON NOT NULL
);"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        """
