package bloomshipper

import (
	"os"
	"path"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/grafana/loki/pkg/logqlmodel/stats"
	v1 "github.com/grafana/loki/pkg/storage/bloom/v1"
	"github.com/grafana/loki/pkg/storage/chunk/cache"
	"github.com/grafana/loki/pkg/storage/stores/shipper/bloomshipper/config"
)

type ClosableBlockQuerier struct {
	*v1.BlockQuerier
	close func() error
}

func (c *ClosableBlockQuerier) Close() error {
	if c.close != nil {
		return c.close()
	}
	return nil
}

func NewBlocksCache(config config.Config, reg prometheus.Registerer, logger log.Logger) *cache.EmbeddedCache[string, BlockDirectory] {
	return cache.NewTypedEmbeddedCache[string, BlockDirectory](
		"bloom-blocks-cache",
		config.BlocksCache.EmbeddedCacheConfig,
		reg,
		logger,
		stats.BloomBlocksCache,
		calculateBlockDirectorySize,
		removeBlockDirectory,
	)
}

func NewBlockDirectory(ref BlockRef, path string, logger log.Logger) BlockDirectory {
	return BlockDirectory{
		BlockRef:                    ref,
		Path:                        path,
		refCount:                    atomic.NewInt32(0),
		removeDirectoryTimeout:      10 * time.Second,
		activeQueriersCheckInterval: 100 * time.Millisecond,
		logger:                      logger,
	}
}

// A BlockDirectory is a local file path that contains a bloom block.
// It maintains a counter for currently active readers.
type BlockDirectory struct {
	BlockRef
	Path                        string
	refCount                    *atomic.Int32
	removeDirectoryTimeout      time.Duration
	activeQueriersCheckInterval time.Duration
	logger                      log.Logger
}

// Convenience function to create a new block from a directory.
// Must not be called outside of BlockQuerier().
func (b BlockDirectory) Block() *v1.Block {
	return v1.NewBlock(v1.NewDirectoryBlockReader(b.Path))
}

// Acquire increases the ref counter on the directory.
func (b BlockDirectory) Acquire() {
	_ = b.refCount.Inc()
}

// Release decreases the ref counter on the directory.
func (b BlockDirectory) Release() error {
	_ = b.refCount.Dec()
	return nil
}

// BlockQuerier returns a new block querier from the directory.
// It increments the counter of active queriers for this directory.
// The counter is decreased when the returned querier is closed.
func (b BlockDirectory) BlockQuerier() *ClosableBlockQuerier {
	b.Acquire()
	return &ClosableBlockQuerier{
		BlockQuerier: v1.NewBlockQuerier(b.Block()),
		close:        b.Release,
	}
}

func calculateBlockDirectorySize(entry *cache.Entry[string, BlockDirectory]) uint64 {
	value := entry.Value
	bloomFileStats, _ := os.Lstat(path.Join(value.Path, v1.BloomFileName))
	seriesFileStats, _ := os.Lstat(path.Join(value.Path, v1.SeriesFileName))
	return uint64(bloomFileStats.Size() + seriesFileStats.Size())
}

// removeBlockDirectory is called by the cache when an item is evicted
// The cache key and the cache value are passed to this function.
// The function needs to be synchronous, because otherwise we could get a cache
// race condition where the item is already evicted from the cache, but the
// underlying directory isn't.
func removeBlockDirectory(_ string, b BlockDirectory) {
	timeout := time.After(b.removeDirectoryTimeout)
	ticker := time.NewTicker(b.activeQueriersCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if b.refCount.Load() == 0 {
				if err := os.RemoveAll(b.Path); err != nil {
					level.Error(b.logger).Log("msg", "error deleting block directory", "err", err)
				}
				return
			}
		case <-timeout:
			level.Warn(b.logger).Log("msg", "force deleting block folder after timeout", "timeout", b.removeDirectoryTimeout)
			if err := os.RemoveAll(b.Path); err != nil {
				level.Error(b.logger).Log("msg", "error force deleting block directory", "err", err)
			}
			return
		}
	}
}
