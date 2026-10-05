-- v10 (compatible with v3+): Add intbox roster, group info and history bypass tables
CREATE TABLE whatsapp_intbox_roster (
    bridge_id      TEXT    NOT NULL,
    group_jid      TEXT    NOT NULL,
    member_jid     TEXT    NOT NULL,
    lid            TEXT    NOT NULL,
    phone          TEXT    NOT NULL,
    name           TEXT    NOT NULL,
    is_admin       BOOLEAN NOT NULL,
    is_super_admin BOOLEAN NOT NULL,
    updated_at     BIGINT  NOT NULL,

    PRIMARY KEY (bridge_id, group_jid, member_jid)
);

CREATE TABLE whatsapp_intbox_group_info (
    bridge_id           TEXT    NOT NULL,
    group_jid           TEXT    NOT NULL,
    total_member_count  INTEGER NOT NULL,
    synced_member_count INTEGER NOT NULL,
    updated_at          BIGINT  NOT NULL,
    -- The counts last published in the com.zeldin.group_info state event (-1 = never sent)
    sent_total_count    INTEGER NOT NULL DEFAULT -1,
    sent_synced_count   INTEGER NOT NULL DEFAULT -1,

    PRIMARY KEY (bridge_id, group_jid)
);

CREATE TABLE whatsapp_intbox_history (
    bridge_id     TEXT   NOT NULL,
    user_login_id TEXT   NOT NULL,
    chat_jid      TEXT   NOT NULL,
    sender_jid    TEXT   NOT NULL,
    message_id    TEXT   NOT NULL,
    timestamp     BIGINT NOT NULL,
    data          bytea  NOT NULL,
    inserted_time BIGINT NOT NULL,

    PRIMARY KEY (bridge_id, user_login_id, chat_jid, sender_jid, message_id),
    CONSTRAINT whatsapp_intbox_history_user_login_fkey FOREIGN KEY (bridge_id, user_login_id)
        REFERENCES user_login (bridge_id, id) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX whatsapp_intbox_history_order_idx ON whatsapp_intbox_history (bridge_id, user_login_id, chat_jid, timestamp, message_id, sender_jid);
CREATE INDEX whatsapp_intbox_history_inserted_idx ON whatsapp_intbox_history (bridge_id, inserted_time);

CREATE INDEX whatsapp_history_sync_message_inserted_idx ON whatsapp_history_sync_message (bridge_id, inserted_time);
