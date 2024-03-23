package article

import (
	"context"
	intrv1 "github.com/hoysics/geektime-basic-go/homework12/proto/gen/intr/v1"
)

// repository 还是要用来操作缓存和DAO
// 事务概念应该在 DAO 这一层

type InteractiveRepository interface {
	Like(ctx context.Context, biz string, like bool, id int64, uid int64) error
}

type CachedInteractiveRepository struct {
	intrSvc intrv1.InteractiveServiceClient
}

func (repo *CachedInteractiveRepository) Like(ctx context.Context, biz string, like bool, id int64, uid int64) error {
	var err error
	if like {
		_, err = repo.intrSvc.Like(ctx, &intrv1.LikeRequest{
			Biz: biz, BizId: id, Uid: uid,
		})
	} else {
		_, err = repo.intrSvc.CancelLike(ctx, &intrv1.CancelLikeRequest{
			Biz: biz, BizId: id, Uid: uid,
		})
	}
	return err
}
