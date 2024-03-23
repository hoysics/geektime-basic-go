package repository

import (
	"context"
	"github.com/ecodeclub/ekit/slice"
	"github.com/hoysics/geektime-basic-go/homework18/domain"
	"github.com/hoysics/geektime-basic-go/homework18/repository/dao"
	"sort"
)

type articleRepository struct {
	dao  dao.ArticleDAO
	tags dao.TagDAO
}

func (a *articleRepository) SearchArticle(ctx context.Context,
	uid int64,
	keywords []string) ([]domain.Article, error) {
	// 标签命中了的
	ids, err := a.tags.Search(ctx, uid, "article", keywords)
	if err != nil {
		return nil, err
	}
	// 加一个 bizids 的输入，这个 bizid 是标签含有关键字的 biz_id
	arts, err := a.dao.Search(ctx, ids, keywords)
	if err != nil {
		return nil, err
	}

	// 将文章按照收藏数和点赞数进行排序
	sort.Slice(arts, func(i, j int) bool {
		// 比较收藏数，收藏数高的排在前面
		if arts[i].CollectCount > arts[j].CollectCount {
			return true
		} else if arts[i].CollectCount < arts[j].CollectCount {
			return false
		}

		// 收藏数相同时，比较点赞数，点赞数高的排在前面
		if arts[i].LikeCount > arts[j].LikeCount {
			return true
		} else if arts[i].LikeCount < arts[j].LikeCount {
			return false
		}

		// 最后按照标签数量排序，标签数量少的排在前面
		return len(arts[i].Tags) < len(arts[j].Tags)
	})

	// 转换成 domain.Article 类型并返回结果
	return slice.Map(arts, func(idx int, src dao.Article) domain.Article {
		return domain.Article{
			Id:           src.Id,
			Title:        src.Title,
			Status:       src.Status,
			Content:      src.Content,
			Tags:         src.Tags,
			CollectCount: src.CollectCount,
			LikeCount:    src.LikeCount,
		}
	}), nil
}

func (a *articleRepository) InputArticle(ctx context.Context, msg domain.Article) error {
	return a.dao.InputArticle(ctx, dao.Article{
		Id:      msg.Id,
		Title:   msg.Title,
		Status:  msg.Status,
		Content: msg.Content,
	})
}

func NewArticleRepository(d dao.ArticleDAO, td dao.TagDAO) ArticleRepository {
	return &articleRepository{
		dao:  d,
		tags: td,
	}
}
