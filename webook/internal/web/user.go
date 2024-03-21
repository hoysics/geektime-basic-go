package web

import (
	"errors"
	"fmt"
	regexp "github.com/dlclark/regexp2"
	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/hoysics/geektime-basic-go/webook/internal/domain"
	"github.com/hoysics/geektime-basic-go/webook/internal/service"
	"net/http"
	"time"
)

// UserHandler 我准备在它上面定义跟用户有关的路由
type UserHandler struct {
	svc         *service.UserService
	emailExp    *regexp.Regexp
	passwordExp *regexp.Regexp

	validate *validator.Validate
}

func NewUserHandler(svc *service.UserService) *UserHandler {
	const (
		emailRegexPattern    = "^\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*$"
		passwordRegexPattern = `^(?=.*[A-Za-z])(?=.*\d)(?=.*[$@$!%*#?&])[A-Za-z\d$@$!%*#?&]{8,}$`
	)
	emailExp := regexp.MustCompile(emailRegexPattern, regexp.None)
	passwordExp := regexp.MustCompile(passwordRegexPattern, regexp.None)
	validate := validator.New()
	return &UserHandler{
		svc:         svc,
		emailExp:    emailExp,
		passwordExp: passwordExp,
		validate:    validate,
	}
}

func (u *UserHandler) RegisterRoutesV1(ug *gin.RouterGroup) {
	ug.GET("/profile", u.Profile)
	ug.POST("/signup", u.SignUp)
	ug.POST("/login", u.Login)
	ug.POST("/edit", u.Edit)
}

func (u *UserHandler) RegisterRoutes(server *gin.Engine) {
	ug := server.Group("/users")
	ug.GET("/profile", u.Profile)
	ug.POST("/signup", u.SignUp)
	ug.POST("/login", u.Login)
	ug.POST("/edit", u.Edit)
}

func (u *UserHandler) SignUp(ctx *gin.Context) {
	type SignUpReq struct {
		Email           string `json:"email"`
		ConfirmPassword string `json:"confirmPassword"`
		Password        string `json:"password"`
	}

	var req SignUpReq
	// Bind 方法会根据 Content-Type 来解析你的数据到 req 里面
	// 解析错了，就会直接写回一个 400 的错误
	if err := ctx.Bind(&req); err != nil {
		return
	}

	ok, err := u.emailExp.MatchString(req.Email)
	if err != nil {
		ctx.String(http.StatusOK, "系统错误")
		return
	}
	if !ok {
		ctx.String(http.StatusOK, "你的邮箱格式不对")
		return
	}
	if req.ConfirmPassword != req.Password {
		ctx.String(http.StatusOK, "两次输入的密码不一致")
		return
	}
	ok, err = u.passwordExp.MatchString(req.Password)
	if err != nil {
		// 记录日志
		ctx.String(http.StatusOK, "系统错误")
		return
	}
	if !ok {
		ctx.String(http.StatusOK, "密码必须大于8位，包含数字、特殊字符")
		return
	}

	// 调用一下 svc 的方法
	err = u.svc.SignUp(ctx, domain.User{
		Email:    req.Email,
		Password: req.Password,
	})
	if errors.Is(err, service.ErrUserDuplicateEmail) {
		ctx.String(http.StatusOK, "邮箱冲突")
		return
	}
	if err != nil {
		ctx.String(http.StatusOK, "系统异常")
		return
	}

	ctx.String(http.StatusOK, "注册成功")
}

func (u *UserHandler) Login(ctx *gin.Context) {
	type LoginReq struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	var req LoginReq
	if err := ctx.Bind(&req); err != nil {
		return
	}
	user, err := u.svc.Login(ctx, req.Email, req.Password)
	if errors.Is(err, service.ErrInvalidUserOrPassword) {
		ctx.String(http.StatusOK, "用户名或密码不对")
		return
	}
	if err != nil {
		ctx.String(http.StatusOK, "系统错误")
		return
	}

	// 步骤2
	// 在这里登录成功了
	// 设置 session
	sess := sessions.Default(ctx)
	// 我可以随便设置值了
	// 你要放在 session 里面的值
	sess.Set("userId", user.Id)
	sess.Save()
	ctx.String(http.StatusOK, "登录成功")
	return
}

func (u *UserHandler) Edit(ctx *gin.Context) {
	type EditReq struct {
		Email    string    `json:"email"`
		Nickname string    `json:"nickname,omitempty" validate:"min=6,max=10"`
		Birthday time.Time `form:"birthday" time_format:"2006-01-02" time_utc:"1"`
		Intro    string    `json:"intro,omitempty" validate:"min=1,max=100"`
	}

	var req EditReq
	if err := ctx.Bind(&req); err != nil {
		return
	}
	if err := u.validate.Struct(req); err != nil {
		ctx.String(http.StatusOK, fmt.Sprintf(`参数不符合规则: %s`, err.Error()))
		return
	}
	// 调用一下 svc 的方法
	err := u.svc.Edit(ctx, domain.User{
		Email:    req.Email,
		Nickname: req.Nickname,
		Birthday: req.Birthday,
		Intro:    req.Intro,
	})
	if err != nil {
		ctx.String(http.StatusOK, "系统异常")
		return
	}

	ctx.String(http.StatusOK, "编辑成功")
}

func (u *UserHandler) Profile(ctx *gin.Context) {
	type ProfileReq struct {
		Email string `json:"email"`
	}
	var req ProfileReq
	if err := ctx.Bind(&req); err != nil {
		return
	}
	if err := u.validate.Struct(req); err != nil {
		ctx.String(http.StatusOK, fmt.Sprintf(`参数不符合规则: %s`, err.Error()))
		return
	}
	// 调用一下 svc 的方法
	profile, err := u.svc.FindByEmail(ctx, req.Email)
	if err != nil {
		ctx.String(http.StatusOK, "系统异常")
		return
	}

	ctx.JSON(
		http.StatusOK,
		gin.H{
			"nickname": profile.Nickname,
			"birthday": profile.Birthday,
			"intro":    profile.Intro,
		},
	)
}
