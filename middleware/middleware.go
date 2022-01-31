package middleware

import "go.uber.org/zap"

// Logger is the global logger.
// both of framework and user's app.
var Logger *zap.SugaredLogger

func init() {
	// Set up logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	Logger = logger.Sugar()
}
