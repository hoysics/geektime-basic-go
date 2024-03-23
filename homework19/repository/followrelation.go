package repository

import (
	"context"
	"github.com/hoysics/geektime-basic-go/homework19/domain"
	"github.com/hoysics/geektime-basic-go/homework19/repository/cache"
	"github.com/hoysics/geektime-basic-go/homework19/repository/dao"
	"github.com/hoysics/geektime-basic-go/homework7/pkg/logger"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type FollowRepository interface {
	// GetFollowee 获取某人的关注列表
	GetFollowee(ctx context.Context, follower, offset, limit int64) ([]domain.FollowRelation, error)
	// FollowInfo 查看关注人的详情
	FollowInfo(ctx context.Context, follower int64, followee int64) (domain.FollowRelation, error)
	// AddFollowRelation 创建关注关系
	AddFollowRelation(ctx context.Context, f domain.FollowRelation) error
	// InactiveFollowRelation 取消关注
	InactiveFollowRelation(ctx context.Context, follower int64, followee int64) error
	GetFollowStatics(ctx context.Context, uid int64) (domain.FollowStatics, error)
}

type CachedRelationRepository struct {
	dao   dao.FollowRelationDao
	cache cache.FollowCache
	l     logger.LoggerV1
}

func (c *CachedRelationRepository) watchCanal() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "password",
	}

	syncer := replication.NewBinlogSyncer(cfg)

	streamer, _ := syncer.StartSync(mysql.Position{})

	for {
		ev, _ := streamer.GetEvent(context.Background())
		// 处理 binlog 事件
		switch e := ev.Event.(type) {
		case *replication.RowsEvent:
			follower := event.Rows[0][0].(int64)
			followee := event.Rows[0][1].(int64)

			// 调用 CachedRelationRepository 处理关注关系
			err := c.Follow(context.Background(), follower, followee)
			if err != nil {
				// 处理错误
			}
		}
	}
}
func (c *CachedRelationRepository) Follow(ctx context.Context, follower, followee int64) error {
	// 更新缓存：增加 A 的关注人数
	followerStatics, err := c.cache.StaticsInfo(ctx, follower)
	if err != nil {
		return err
	}
	followerStatics.Followees++
	err = c.cache.SetStaticsInfo(ctx, follower, followerStatics)
	if err != nil {
		return err
	}

	// 更新缓存：增加 B 的粉丝数
	followeeStatics, err := c.cache.StaticsInfo(ctx, followee)
	if err != nil {
		return err
	}
	followeeStatics.Followers++
	err = c.cache.SetStaticsInfo(ctx, followee, followeeStatics)
	if err != nil {
		return err
	}

	return nil
}

func (c *CachedRelationRepository) CancelFollow(ctx context.Context, follower, followee int64) error {
	// 更新状态为取消关注
	err := c.dao.UpdateStatus(ctx, followee, follower, 0)
	if err != nil {
		return err
	}

	// 更新缓存：减少 A 的关注人数
	followerStatics, err := c.cache.StaticsInfo(ctx, follower)
	if err != nil {
		return err
	}
	followerStatics.Followees--
	err = c.cache.SetStaticsInfo(ctx, follower, followerStatics)
	if err != nil {
		return err
	}

	// 更新缓存：减少 B 的粉丝数
	followeeStatics, err := c.cache.StaticsInfo(ctx, followee)
	if err != nil {
		return err
	}
	followeeStatics.Followers--
	err = c.cache.SetStaticsInfo(ctx, followee, followeeStatics)
	if err != nil {
		return err
	}

	return nil
}

func (c *CachedRelationRepository) GetFollowStatics(ctx context.Context, uid int64) (domain.FollowStatics, error) {
	// 快路径
	res, err := c.cache.StaticsInfo(ctx, uid)
	if err == nil {
		return res, err
	}
	// 慢路径
	res.Followers, err = c.dao.CntFollower(ctx, uid)
	if err != nil {
		return res, err
	}
	res.Followees, err = c.dao.CntFollowee(ctx, uid)
	if err != nil {
		return res, err
	}
	err = c.cache.SetStaticsInfo(ctx, uid, res)
	if err != nil {
		// 这里记录日志
		c.l.Error("缓存关注统计信息失败",
			logger.Error(err),
			logger.Int64("uid", uid))
	}
	return res, nil
}

func (c *CachedRelationRepository) InactiveFollowRelation(ctx context.Context, follower int64, followee int64) error {
	err := c.dao.UpdateStatus(ctx, followee, follower, dao.FollowRelationStatusInactive)
	if err != nil {
		return err
	}
	return c.cache.CancelFollow(ctx, follower, followee)
}

func (c *CachedRelationRepository) GetFollowee(ctx context.Context, follower, offset, limit int64) ([]domain.FollowRelation, error) {
	// 你要做缓存，撑死了就是缓存第一页
	// 缓存命中率贼低
	followerList, err := c.dao.FollowRelationList(ctx, follower, offset, limit)
	if err != nil {
		return nil, err
	}
	return c.genFollowRelationList(followerList), nil
}

func (c *CachedRelationRepository) genFollowRelationList(followerList []dao.FollowRelation) []domain.FollowRelation {
	res := make([]domain.FollowRelation, 0, len(followerList))
	for _, c := range followerList {
		res = append(res, c.toDomain(c))
	}
	return res
}

func (c *CachedRelationRepository) FollowInfo(ctx context.Context, follower int64, followee int64) (domain.FollowRelation, error) {
	// 要比列表有缓存价值
	c, err := c.dao.FollowRelationDetail(ctx, follower, followee)
	if err != nil {
		return domain.FollowRelation{}, err
	}
	return c.toDomain(c), nil
}

func (c *CachedRelationRepository) AddFollowRelation(ctx context.Context, c domain.FollowRelation) error {
	err := c.dao.CreateFollowRelation(ctx, c.toEntity(c))
	if err != nil {
		return err
	}
	// 这里要更新在 Redis 上的缓存计数，对于 A 关注了 B 来说，这里要增加 A 的 followee 的数量
	// 同时要增加 B 的 follower 的数量
	return c.cache.Follow(ctx, c.Follower, c.Followee)
}

func (c *CachedRelationRepository) toDomain(fr dao.FollowRelation) domain.FollowRelation {
	return domain.FollowRelation{
		Followee: fr.Followee,
		Follower: fr.Follower,
	}
}

func (c *CachedRelationRepository) toEntity(c domain.FollowRelation) dao.FollowRelation {
	return dao.FollowRelation{
		Followee: c.Followee,
		Follower: c.Follower,
	}
}

func NewFollowRelationRepository(dao dao.FollowRelationDao,
	cache cache.FollowCache, l logger.LoggerV1) FollowRepository {
	return &CachedRelationRepository{
		dao:   dao,
		cache: cache,
		l:     l,
	}
}
