package storage

const (
	metaindexFilename  = "metaindex.bin"
	indexFilename      = "index.bin"
	valuesFilename     = "values.bin"
	timestampsFilename = "timestamps.bin"
	PartsFilename      = "parts.json"
	metadataFilename   = "metadata.json"

	appliedRetentionFilename    = "appliedRetention.txt"
	resetCacheOnStartupFilename = "reset_cache_on_startup"
)

const (
	smallDirname = "small"
	bigDirname   = "big"

	IndexdbDirname   = "indexdb"
	DataDirname      = "data"
	metadataDirname  = "metadata"
	snapshotsDirname = "snapshots"
	cacheDirname     = "cache"
	TempName         = ".must-remove."
)
