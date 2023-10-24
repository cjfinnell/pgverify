package pgverify

import (
	"context"

	"github.com/jackc/pgx/v5/tracelog"
	"github.com/sirupsen/logrus"
)

var _ tracelog.Logger = (*pgxLogger)(nil)

type pgxLogger struct {
	l logrus.FieldLogger
}

func (p *pgxLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
	parsedLevel, err := logrus.ParseLevel(level.String())
	if err != nil {
		parsedLevel = logrus.ErrorLevel
	}

	p.l.WithFields(data).Log(parsedLevel, msg)
}

func NewTraceLogger(l *logrus.Entry) *tracelog.TraceLog {
	logLevel, err := tracelog.LogLevelFromString(l.Level.String())
	if err != nil {
		l.WithError(err).Warn("failed to parse log level, defaulting to error")

		logLevel = tracelog.LogLevelError
	}

	return &tracelog.TraceLog{
		Logger:   &pgxLogger{l},
		LogLevel: logLevel,
	}
}
