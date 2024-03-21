package service

import (
	"context"
	"errors"
	"github.com/hoysics/geektime-basic-go/webook/internal/domain"
	"github.com/hoysics/geektime-basic-go/webook/internal/repository"
	"golang.org/x/crypto/bcrypt"
)

var ErrUserDuplicateEmail = repository.ErrUserDuplicateEmail
var ErrInvalidUserOrPassword = errors.New("账号/邮箱或密码不对")

type UserService struct {
	repo *repository.UserRepository
}

func NewUserService(repo *repository.UserRepository) *UserService {
	return &UserService{
		repo: repo,
	}
}

func (svc *UserService) Login(ctx context.Context, email, password string) (domain.User, error) {
	// 先找用户
	u, err := svc.repo.FindByEmail(ctx, email)
	if errors.Is(err, repository.ErrUserNotFound) {
		return domain.User{}, ErrInvalidUserOrPassword
	}
	if err != nil {
		return domain.User{}, err
	}
	// 比较密码了
	err = bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(password))
	if err != nil {
		// DEBUG
		return domain.User{}, ErrInvalidUserOrPassword
	}
	return u, nil
}

func (svc *UserService) SignUp(ctx context.Context, u domain.User) error {
	// 你要考虑加密放在哪里的问题了
	hash, err := bcrypt.GenerateFromPassword([]byte(u.Password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	u.Password = string(hash)
	// 然后就是，存起来
	return svc.repo.Create(ctx, u)
}

func (svc *UserService) Edit(ctx context.Context, u domain.User) error {
	// 然后就是，存起来
	return svc.repo.Save(ctx, u)
}

func (svc *UserService) FindByEmail(ctx context.Context, email string) (domain.User, error) {
	// 然后就是，存起来
	return svc.repo.FindByEmail(ctx, email)
}
