package article

import (
	"context"
	"github.com/hoysics/geektime-basic-go/homework7/internal/domain"
)

type ArticleReaderRepository interface {
	// Save 有就更新，没有就新建，即 upsert 的语义
	Save(ctx context.Context, art domain.Article) (int64, error)
}
