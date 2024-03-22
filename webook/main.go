package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func main() {
	//db := initDB()
	//rdb := initRedis()
	//
	//server := initWebServer()
	//
	//u := initUser(db, rdb)
	//u.RegisterRoutes(server)

	server := InitWebServer()

	server.GET("/hello", func(ctx *gin.Context) {
		ctx.String(http.StatusOK, "你好，你来了")
	})

	server.Run(":8080")
}
