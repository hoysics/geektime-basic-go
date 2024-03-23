package article

import (
	"context"
	"github.com/hoysics/geektime-basic-go/homework7/internal/domain"
)

type ArticleAuthorRepository interface {
	Create(ctx context.Context, art domain.Article) (int64, error)
	Update(ctx context.Context, art domain.Article) error
}
