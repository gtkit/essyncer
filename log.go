package essyncer

import "go.uber.org/zap"

// Logger 定义日志接口，签名与 gtkit/logger 原生 zap.Field API 兼容。
type Logger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
}

type zapLogger struct{ l *zap.Logger }

func (z *zapLogger) Info(msg string, fields ...zap.Field)  { z.l.Info(msg, fields...) }
func (z *zapLogger) Warn(msg string, fields ...zap.Field)  { z.l.Warn(msg, fields...) }
func (z *zapLogger) Error(msg string, fields ...zap.Field) { z.l.Error(msg, fields...) }
func (z *zapLogger) Debug(msg string, fields ...zap.Field) { z.l.Debug(msg, fields...) }

func defaultLogger() Logger {
	l, _ := zap.NewProduction()
	if l == nil {
		l = zap.NewNop()
	}
	return &zapLogger{l: l}
}
