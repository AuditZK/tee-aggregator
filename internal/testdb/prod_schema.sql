CREATE TYPE public."SyncStatusEnum" AS ENUM (
    'pending',
    'syncing',
    'completed',
    'error'
);

CREATE TABLE public.data_encryption_keys (
    id text NOT NULL,
    "encryptedDEK" text NOT NULL,
    iv text NOT NULL,
    "authTag" text NOT NULL,
    "keyVersion" text NOT NULL,
    "masterKeyId" text NOT NULL,
    "isActive" boolean DEFAULT true NOT NULL,
    "rotatedAt" timestamp(3) without time zone,
    "createdAt" timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "updatedAt" timestamp(3) without time zone NOT NULL
);

CREATE TABLE public.exchange_connections (
    id text NOT NULL,
    "userUid" text NOT NULL,
    exchange text NOT NULL,
    label text NOT NULL,
    "encryptedApiKey" text NOT NULL,
    "encryptedApiSecret" text NOT NULL,
    "encryptedPassphrase" text,
    "credentialsHash" text,
    "isActive" boolean DEFAULT true NOT NULL,
    "syncIntervalMinutes" integer DEFAULT 1440 NOT NULL,
    "createdAt" timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "updatedAt" timestamp(3) without time zone NOT NULL,
    "excludeFromReport" boolean DEFAULT false NOT NULL,
    "isPaper" boolean,
    "kycLevel" text,
    "rebuildFinalizedAt" timestamp with time zone,
    "rebuildRequestedAt" timestamp with time zone
);

CREATE TABLE public.migrations (
    id integer NOT NULL,
    name text NOT NULL,
    "appliedAt" timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE SEQUENCE public.migrations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.migrations_id_seq OWNED BY public.migrations.id;

CREATE TABLE public.schema_migrations (
    version integer NOT NULL,
    name text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.signed_reports (
    id text NOT NULL,
    "reportId" text NOT NULL,
    "userUid" text NOT NULL,
    "startDate" timestamp(3) without time zone NOT NULL,
    "endDate" timestamp(3) without time zone NOT NULL,
    benchmark text,
    "reportData" jsonb NOT NULL,
    signature text NOT NULL,
    "reportHash" text NOT NULL,
    "enclaveVersion" text NOT NULL,
    "createdAt" timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE public.snapshot_data (
    id text NOT NULL,
    "userUid" text NOT NULL,
    "timestamp" timestamp(3) without time zone NOT NULL,
    exchange text NOT NULL,
    "totalEquity" double precision NOT NULL,
    "realizedBalance" double precision NOT NULL,
    "unrealizedPnL" double precision NOT NULL,
    deposits double precision DEFAULT 0 NOT NULL,
    withdrawals double precision DEFAULT 0 NOT NULL,
    breakdown_by_market jsonb,
    "createdAt" timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "updatedAt" timestamp(3) without time zone NOT NULL,
    label text DEFAULT ''::text NOT NULL,
    from_external_rebuilder boolean DEFAULT false NOT NULL
);

CREATE TABLE public.sync_rate_limit_logs (
    id text NOT NULL,
    "userUid" text NOT NULL,
    exchange text NOT NULL,
    "lastSyncTime" timestamp(3) without time zone NOT NULL,
    "syncCount" integer DEFAULT 1 NOT NULL,
    "createdAt" timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "updatedAt" timestamp(3) without time zone NOT NULL,
    label text DEFAULT ''::text NOT NULL
);

CREATE TABLE public.sync_statuses (
    id text NOT NULL,
    "userUid" text NOT NULL,
    exchange text NOT NULL,
    "lastSyncTime" timestamp(3) without time zone,
    status public."SyncStatusEnum" DEFAULT 'pending'::public."SyncStatusEnum" NOT NULL,
    "totalTrades" integer DEFAULT 0 NOT NULL,
    "errorMessage" text,
    "createdAt" timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "updatedAt" timestamp(3) without time zone NOT NULL,
    label text DEFAULT ''::text NOT NULL
);

CREATE TABLE public.users (
    id text NOT NULL,
    uid text NOT NULL,
    "syncIntervalMinutes" integer DEFAULT 1440 NOT NULL,
    "createdAt" timestamp(3) without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "updatedAt" timestamp(3) without time zone NOT NULL,
    "platformHash" text
);

ALTER TABLE ONLY public.migrations ALTER COLUMN id SET DEFAULT nextval('public.migrations_id_seq'::regclass);

ALTER TABLE ONLY public.data_encryption_keys
    ADD CONSTRAINT data_encryption_keys_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.exchange_connections
    ADD CONSTRAINT exchange_connections_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.migrations
    ADD CONSTRAINT migrations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);

ALTER TABLE ONLY public.signed_reports
    ADD CONSTRAINT signed_reports_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.snapshot_data
    ADD CONSTRAINT snapshot_data_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.sync_rate_limit_logs
    ADD CONSTRAINT sync_rate_limit_logs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.sync_statuses
    ADD CONSTRAINT sync_statuses_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT "users_platformHash_key" UNIQUE ("platformHash");

CREATE INDEX "data_encryption_keys_isActive_idx" ON public.data_encryption_keys USING btree ("isActive");

CREATE INDEX "data_encryption_keys_masterKeyId_idx" ON public.data_encryption_keys USING btree ("masterKeyId");

CREATE INDEX "exchange_connections_userUid_exchange_idx" ON public.exchange_connections USING btree ("userUid", exchange);

CREATE UNIQUE INDEX "exchange_connections_userUid_exchange_label_key" ON public.exchange_connections USING btree ("userUid", exchange, label);

CREATE INDEX idx_users_uid ON public.users USING btree (uid);

CREATE UNIQUE INDEX migrations_name_key ON public.migrations USING btree (name);

CREATE INDEX "signed_reports_reportId_idx" ON public.signed_reports USING btree ("reportId");

CREATE UNIQUE INDEX "signed_reports_reportId_key" ON public.signed_reports USING btree ("reportId");

CREATE INDEX "signed_reports_userUid_idx" ON public.signed_reports USING btree ("userUid");

CREATE UNIQUE INDEX "signed_reports_userUid_startDate_endDate_benchmark_key" ON public.signed_reports USING btree ("userUid", "startDate", "endDate", benchmark);

CREATE INDEX idx_signed_reports_user_created ON public.signed_reports USING btree ("userUid", "createdAt" DESC);

CREATE INDEX snapshot_data_exchange_timestamp_idx ON public.snapshot_data USING btree (exchange, "timestamp");

CREATE UNIQUE INDEX "snapshot_data_userUid_timestamp_exchange_label_key" ON public.snapshot_data USING btree ("userUid", "timestamp", exchange, label);

CREATE INDEX "snapshot_data_userUid_timestamp_idx" ON public.snapshot_data USING btree ("userUid", "timestamp");

CREATE INDEX "sync_rate_limit_logs_lastSyncTime_idx" ON public.sync_rate_limit_logs USING btree ("lastSyncTime");

CREATE UNIQUE INDEX "sync_rate_limit_logs_userUid_exchange_label_key" ON public.sync_rate_limit_logs USING btree ("userUid", exchange, label);

CREATE UNIQUE INDEX "sync_statuses_userUid_exchange_label_key" ON public.sync_statuses USING btree ("userUid", exchange, label);

CREATE UNIQUE INDEX users_uid_key ON public.users USING btree (uid);

ALTER TABLE ONLY public.exchange_connections
    ADD CONSTRAINT "exchange_connections_userUid_fkey" FOREIGN KEY ("userUid") REFERENCES public.users(uid) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY public.snapshot_data
    ADD CONSTRAINT "snapshot_data_userUid_fkey" FOREIGN KEY ("userUid") REFERENCES public.users(uid) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY public.sync_statuses
    ADD CONSTRAINT "sync_statuses_userUid_fkey" FOREIGN KEY ("userUid") REFERENCES public.users(uid) ON UPDATE CASCADE ON DELETE CASCADE;
